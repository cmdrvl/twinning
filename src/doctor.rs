use serde::Serialize;

use crate::{
    cli::{DoctorArgs, DoctorCommand},
    report::REPORT_VERSION,
    runtime::Execution,
};

const TOOL_NAME: &str = "twinning";
const HEALTH_MODE: &str = "doctor_health";
const CAPABILITIES_MODE: &str = "doctor_capabilities";
const TRIAGE_MODE: &str = "doctor_triage";

pub fn execute(
    args: &DoctorArgs,
    json_mode: bool,
) -> Result<Execution, Box<dyn std::error::Error>> {
    if args.robot_triage {
        let report = triage_report();
        return Ok(Execution {
            exit_code: 0,
            stdout: render_json(&report)?,
        });
    }

    match args.command.unwrap_or(DoctorCommand::Health) {
        DoctorCommand::Health => {
            let report = health_report();
            Ok(Execution {
                exit_code: 0,
                stdout: if json_mode {
                    render_json(&report)?
                } else {
                    render_health_human(&report)
                },
            })
        }
        DoctorCommand::Capabilities => {
            let report = capabilities_report();
            Ok(Execution {
                exit_code: 0,
                stdout: if json_mode {
                    render_json(&report)?
                } else {
                    render_capabilities_human(&report)
                },
            })
        }
        DoctorCommand::RobotDocs => Ok(Execution {
            exit_code: 0,
            stdout: robot_docs(),
        }),
    }
}

#[derive(Debug, Clone, Serialize)]
struct HealthReport {
    version: &'static str,
    outcome: &'static str,
    mode: &'static str,
    tool: &'static str,
    package_version: &'static str,
    status: &'static str,
    read_only: bool,
    side_effects: SideEffects,
    checks: Vec<DoctorCheck>,
    recommendations: Vec<Recommendation>,
    next_step: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct CapabilitiesReport {
    version: &'static str,
    outcome: &'static str,
    mode: &'static str,
    tool: &'static str,
    package_version: &'static str,
    read_only: bool,
    commands: Vec<CommandCapability>,
    output_contracts: Vec<OutputContract>,
    fix_mode: FixMode,
    side_effects: SideEffects,
    next_step: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct TriageReport {
    version: &'static str,
    outcome: &'static str,
    mode: &'static str,
    tool: &'static str,
    package_version: &'static str,
    summary: TriageSummary,
    findings: Vec<DoctorCheck>,
    recommended_next_work: Vec<Recommendation>,
    side_effects: SideEffects,
}

#[derive(Debug, Clone, Serialize)]
struct TriageSummary {
    health: &'static str,
    doctor_surface: &'static str,
    release_gate: &'static str,
    fix_mode: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct SideEffects {
    reads_schema_files: bool,
    reads_snapshot_files: bool,
    reads_verify_artifacts: bool,
    binds_network_socket: bool,
    runs_child_command: bool,
    writes_reports: bool,
    writes_snapshots: bool,
    writes_doctor_artifacts: bool,
    mutates_repository: bool,
    changes_cwd: bool,
    uses_network: bool,
}

#[derive(Debug, Clone, Serialize)]
struct DoctorCheck {
    id: &'static str,
    status: &'static str,
    severity: &'static str,
    message: &'static str,
    next_command: Option<&'static str>,
}

#[derive(Debug, Clone, Serialize)]
struct Recommendation {
    id: &'static str,
    priority: &'static str,
    action: &'static str,
    reason: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct CommandCapability {
    command: &'static str,
    output: &'static str,
    read_only: bool,
    description: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct OutputContract {
    name: &'static str,
    version: &'static str,
    description: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct FixMode {
    available: bool,
    reason: &'static str,
    required_before_enablement: Vec<&'static str>,
}

fn health_report() -> HealthReport {
    HealthReport {
        version: REPORT_VERSION,
        outcome: "READY",
        mode: HEALTH_MODE,
        tool: TOOL_NAME,
        package_version: env!("CARGO_PKG_VERSION"),
        status: "healthy",
        read_only: true,
        side_effects: SideEffects::read_only(),
        checks: vec![
            DoctorCheck {
                id: "doctor_surface",
                status: "pass",
                severity: "info",
                message: "read-only doctor health, capabilities, robot-docs, and robot-triage commands are available",
                next_command: Some("twinning doctor capabilities --json"),
            },
            DoctorCheck {
                id: "describe_without_engine",
                status: "pass",
                severity: "info",
                message: "`twinning --describe` is available without requiring an engine subcommand",
                next_command: Some("twinning --describe"),
            },
            DoctorCheck {
                id: "bootstrap_contract",
                status: "pass",
                severity: "info",
                message: "doctor mode does not parse schema files, bind pgwire, run child commands, or emit report/snapshot artifacts",
                next_command: Some("twinning postgres --schema schema.sql --json"),
            },
            DoctorCheck {
                id: "fix_mode",
                status: "not_available",
                severity: "info",
                message: "doctor --fix is intentionally absent until detector, backup, inverse, and fixture coverage exist",
                next_command: None,
            },
        ],
        recommendations: detector_recommendations(),
        next_step: "Use `twinning doctor --robot-triage` for machine-readable follow-up work before any fix mode is added.",
    }
}

fn capabilities_report() -> CapabilitiesReport {
    CapabilitiesReport {
        version: REPORT_VERSION,
        outcome: "READY",
        mode: CAPABILITIES_MODE,
        tool: TOOL_NAME,
        package_version: env!("CARGO_PKG_VERSION"),
        read_only: true,
        commands: vec![
            CommandCapability {
                command: "twinning doctor health --json",
                output: "twinning.v0 doctor_health JSON",
                read_only: true,
                description: "Summarizes doctor availability, read-only guarantees, and next diagnostic steps.",
            },
            CommandCapability {
                command: "twinning doctor capabilities --json",
                output: "twinning.v0 doctor_capabilities JSON",
                read_only: true,
                description: "Lists agent-facing commands, output contracts, and safety boundaries.",
            },
            CommandCapability {
                command: "twinning doctor robot-docs",
                output: "plain text",
                read_only: true,
                description: "Prints concise command guidance for headless agents.",
            },
            CommandCapability {
                command: "twinning doctor --robot-triage",
                output: "twinning.v0 doctor_triage JSON",
                read_only: true,
                description: "Emits structured findings and recommended follow-up work.",
            },
            CommandCapability {
                command: "twinning --describe",
                output: "operator.v0 JSON",
                read_only: true,
                description: "Prints the compiled operator manifest without requiring bootstrap inputs.",
            },
            CommandCapability {
                command: "twinning postgres --schema <FILE> --json",
                output: "twinning.v0 bootstrap JSON",
                read_only: true,
                description: "Validates a Postgres bootstrap schema and emits the existing status report.",
            },
        ],
        output_contracts: vec![
            OutputContract {
                name: "twinning status report",
                version: "twinning.v0",
                description: "Bootstrap, run_once, refusal, and doctor JSON use the same top-level version vocabulary.",
            },
            OutputContract {
                name: "operator manifest",
                version: "operator.v0",
                description: "`--describe` prints the compiled operator manifest.",
            },
            OutputContract {
                name: "bootstrap snapshot",
                version: "twinning.snapshot.v0",
                description: "Snapshot output remains exclusive to bootstrap/run paths, not doctor paths.",
            },
        ],
        fix_mode: FixMode {
            available: false,
            reason: "read-only campaign slice only; no mutation chokepoint or undo artifact is exposed yet",
            required_before_enablement: vec![
                "fixture-backed detectors",
                "verbatim backups for every mutation",
                "explicit inverse operations",
                "undo tests that prove byte-identical restoration",
            ],
        },
        side_effects: SideEffects::read_only(),
        next_step: "File or complete detector fixtures before adding any doctor --fix command.",
    }
}

fn triage_report() -> TriageReport {
    TriageReport {
        version: REPORT_VERSION,
        outcome: "READY",
        mode: TRIAGE_MODE,
        tool: TOOL_NAME,
        package_version: env!("CARGO_PKG_VERSION"),
        summary: TriageSummary {
            health: "healthy",
            doctor_surface: "read_only_available",
            release_gate: "run cargo fmt --check, cargo clippy --all-targets -- -D warnings, cargo test, and UBS before release",
            fix_mode: "absent_by_design",
        },
        findings: health_report().checks,
        recommended_next_work: detector_recommendations(),
        side_effects: SideEffects::read_only(),
    }
}

fn detector_recommendations() -> Vec<Recommendation> {
    vec![
        Recommendation {
            id: "schema_detector_fixtures",
            priority: "p3",
            action: "Add fixture-backed detectors for malformed DDL, unsupported SQL shapes, and catalog drift.",
            reason: "Schema and catalog failures are the highest-signal read-only doctor checks for twinning.",
        },
        Recommendation {
            id: "snapshot_detector_fixtures",
            priority: "p3",
            action: "Add detectors for malformed, mismatched, or tampered twinning.snapshot.v0 artifacts.",
            reason: "Snapshot determinism and hash verification are core correctness boundaries.",
        },
        Recommendation {
            id: "live_runtime_detector_fixtures",
            priority: "p3",
            action: "Add detectors for pgwire bind failures, unsupported canary shapes, and run_once child failure metadata.",
            reason: "Future fix mode must not touch live runtime state without exact failure classification.",
        },
    ]
}

impl SideEffects {
    fn read_only() -> Self {
        Self {
            reads_schema_files: false,
            reads_snapshot_files: false,
            reads_verify_artifacts: false,
            binds_network_socket: false,
            runs_child_command: false,
            writes_reports: false,
            writes_snapshots: false,
            writes_doctor_artifacts: false,
            mutates_repository: false,
            changes_cwd: false,
            uses_network: false,
        }
    }
}

fn render_json<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    let mut rendered = serde_json::to_string_pretty(value)?;
    rendered.push('\n');
    Ok(rendered)
}

fn render_health_human(report: &HealthReport) -> String {
    let mut lines = vec![
        format!("{} doctor {}", report.tool, report.status),
        format!("version: {}", report.package_version),
        String::from("read_only: true"),
    ];
    for check in &report.checks {
        lines.push(format!(
            "check: {} [{}] {}",
            check.id, check.status, check.message
        ));
    }
    lines.push(format!("next: {}", report.next_step));
    lines.push(String::new());
    lines.join("\n")
}

fn render_capabilities_human(report: &CapabilitiesReport) -> String {
    let mut lines = vec![
        format!("{} doctor capabilities", report.tool),
        format!("version: {}", report.package_version),
        String::from("read_only: true"),
    ];
    for command in &report.commands {
        lines.push(format!(
            "command: {} -> {}",
            command.command, command.output
        ));
    }
    lines.push(format!("fix_mode: {}", report.fix_mode.available));
    lines.push(format!("next: {}", report.next_step));
    lines.push(String::new());
    lines.join("\n")
}

fn robot_docs() -> String {
    [
        "twinning doctor robot-docs",
        "read_only: true",
        "commands:",
        "  twinning doctor health --json",
        "  twinning doctor capabilities --json",
        "  twinning doctor robot-docs",
        "  twinning doctor --robot-triage",
        "  twinning --describe",
        "notes:",
        "  doctor mode does not read schema, snapshot, or verify files",
        "  doctor mode does not bind sockets, run child commands, or write artifacts",
        "  doctor --fix is intentionally unavailable until detector fixtures, backups, inverses, and undo tests exist",
        "",
    ]
    .join("\n")
}
