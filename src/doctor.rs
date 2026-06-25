use serde::Serialize;

use crate::{
    cli::{DoctorArgs, DoctorCommand},
    paths::{ConfigFootprint, config_footprint},
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
    config_footprint: ConfigFootprint,
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
    config_footprint: ConfigFootprint,
    commands: Vec<CommandCapability>,
    output_contracts: Vec<OutputContract>,
    detectors: Vec<DetectorSpec>,
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
    config_footprint: ConfigFootprint,
    findings: Vec<DoctorCheck>,
    detectors: Vec<DetectorSpec>,
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
struct DetectorSpec {
    id: &'static str,
    failure_mode: &'static str,
    fixture: &'static str,
    signal: DetectorSignal,
    fix_available: bool,
    required_before_fix: Vec<&'static str>,
}

#[derive(Debug, Clone, Serialize)]
struct DetectorSignal {
    kind: &'static str,
    code: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    sqlstate: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    json_path: Option<&'static str>,
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
        config_footprint: config_footprint(),
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
        next_step: "Use `twinning doctor --robot-triage` for machine-readable follow-up work, or start with `twinning postgres --schema schema.sql --json` / `twinning rest --spec openapi.yaml --json` for protocol-specific checks.",
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
        config_footprint: config_footprint(),
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
            CommandCapability {
                command: "twinning rest --spec <FILE> --json",
                output: "twinning.rest-report.v0 JSON",
                read_only: false,
                description: "Starts an OpenAPI 3.x REST twin. Required input is JSON or YAML; key flags are --run, --serve, --report, --canary, and --port (default 8080). V1 REST twin with adaptive routing (flat-crud, schema-first, prefix-scoped topology), response wrapper detection, remote $ref resolution, auth shape compliance, and matched JSON requestBody validation before response stubs/materialization/unsupported_shape. Nested paths handled via ResourceTopology. Bypasses auth credential validation in bypass mode (shape mode enforces credential presence).",
            },
            CommandCapability {
                command: "twinning mcp --server <COMMAND> --json",
                output: "twinning.mcp-report.v0 JSON",
                read_only: false,
                description: "Starts a Model Context Protocol JSON-RPC 2.0 twin from a live MCP server or static manifest. Key flags: --server (live introspection command), --manifest (static JSON manifest), --stdio (stdio transport), --run, --serve, --report, --auth-mode, --port (default 9878). Supports tools, resources, and prompts catalog; auth enforcement; request logging.",
            },
            CommandCapability {
                command: "twinning snowflake --schema <FILE> --json",
                output: "twinning.snowflake-report.v0 JSON",
                read_only: false,
                description: "Starts a Snowflake HTTP wire protocol twin with Arrow IPC rowsetBase64 result encoding. Key flags: --schema, --run, --serve, --report, --materialize-source-url, --port (default 9876). Supports SHOW/DESCRIBE meta-queries, DDL catalog introspection, and Snowflake session lifecycle.",
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
            OutputContract {
                name: "REST session report",
                version: "twinning.rest-report.v0",
                description: "REST sessions report spec identity, exercised endpoints, outcome counts, response-stub hits, refusals, constraint violations, and optional canary assertions.",
            },
            OutputContract {
                name: "MCP session report",
                version: "twinning.mcp-report.v0",
                description: "MCP twins report server identity, catalog hash, tool/resource/prompt counts, session request counts, and tool call outcomes.",
            },
            OutputContract {
                name: "Snowflake session report",
                version: "twinning.snowflake-report.v0",
                description: "Snowflake wire twins report catalog identity, session lifecycle, query dispatch counts, and canary validation results.",
            },
        ],
        detectors: detector_specs(),
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
        config_footprint: config_footprint(),
        findings: health_report().checks,
        detectors: detector_specs(),
        recommended_next_work: detector_recommendations(),
        side_effects: SideEffects::read_only(),
    }
}

fn detector_specs() -> Vec<DetectorSpec> {
    vec![
        DetectorSpec {
            id: "malformed_postgres_ddl",
            failure_mode: "Malformed Postgres DDL must stay a process-level bootstrap refusal.",
            fixture: "tests/fixtures/doctor_detectors/malformed_postgres_ddl/schema.sql",
            signal: DetectorSignal {
                kind: "process_refusal",
                code: "E_SCHEMA_PARSE",
                sqlstate: None,
                json_path: Some("$.refusal.code"),
            },
            fix_available: false,
            required_before_fix: fix_requirements(),
        },
        DetectorSpec {
            id: "unsupported_sql_shape",
            failure_mode: "Unsupported SQL and canary near-misses must stay explicit refusals.",
            fixture: "tests/fixtures/doctor_detectors/unsupported_sql_shape/query.sql",
            signal: DetectorSignal {
                kind: "operation_refusal",
                code: "unsupported_shape",
                sqlstate: Some("0A000"),
                json_path: Some("$.op_type=refusal"),
            },
            fix_available: false,
            required_before_fix: fix_requirements(),
        },
        DetectorSpec {
            id: "catalog_drift",
            failure_mode: "Schema edits that change the normalized catalog must be detectable.",
            fixture: "tests/fixtures/doctor_detectors/catalog_drift/after.sql",
            signal: DetectorSignal {
                kind: "catalog_comparison",
                code: "catalog_hash_drift",
                sqlstate: None,
                json_path: Some("$.catalog"),
            },
            fix_available: false,
            required_before_fix: fix_requirements(),
        },
        DetectorSpec {
            id: "tampered_snapshot",
            failure_mode: "Malformed or tampered twinning.snapshot.v0 artifacts must refuse restore.",
            fixture: "tests/fixtures/doctor_detectors/tampered_snapshot/schema.sql",
            signal: DetectorSignal {
                kind: "process_refusal",
                code: "E_SNAPSHOT_VERIFY",
                sqlstate: None,
                json_path: Some("$.refusal.code"),
            },
            fix_available: false,
            required_before_fix: fix_requirements(),
        },
        DetectorSpec {
            id: "batch_only_verify_artifact",
            failure_mode: "Batch-only verify artifacts must refuse embedded live execution.",
            fixture: "tests/fixtures/doctor_detectors/batch_only_verify_artifact/constraints.verify.json",
            signal: DetectorSignal {
                kind: "process_refusal",
                code: "E_BATCH_ONLY_RULE",
                sqlstate: None,
                json_path: Some("$.refusal.code"),
            },
            fix_available: false,
            required_before_fix: fix_requirements(),
        },
        DetectorSpec {
            id: "pgwire_bind_failure",
            failure_mode: "Listener bind failures must stay explicit runtime I/O refusals.",
            fixture: "tests/fixtures/doctor_detectors/pgwire_bind_failure/schema.sql",
            signal: DetectorSignal {
                kind: "process_refusal",
                code: "E_RUNTIME_IO",
                sqlstate: None,
                json_path: Some("$.refusal.detail.stage"),
            },
            fix_available: false,
            required_before_fix: fix_requirements(),
        },
        DetectorSpec {
            id: "run_once_child_failure_metadata",
            failure_mode: "Non-zero child exits must be preserved in run_once report metadata.",
            fixture: "tests/fixtures/doctor_detectors/run_once_child_failure/schema.sql",
            signal: DetectorSignal {
                kind: "report_failure",
                code: "run_once_child_failed",
                sqlstate: None,
                json_path: Some("$.run.exit_code"),
            },
            fix_available: false,
            required_before_fix: fix_requirements(),
        },
        DetectorSpec {
            id: "malformed_openapi_spec",
            failure_mode: "Syntactically invalid OpenAPI YAML must stay a process-level REST bootstrap refusal.",
            fixture: "tests/fixtures/doctor_detectors/malformed_openapi_spec/spec.yaml",
            signal: DetectorSignal {
                kind: "process_refusal",
                code: "E_OPENAPI_PARSE",
                sqlstate: None,
                json_path: Some("$.refusal.code"),
            },
            fix_available: false,
            required_before_fix: fix_requirements(),
        },
        DetectorSpec {
            id: "circular_ref_spec",
            failure_mode: "Circular or nested OpenAPI $ref chains must stay process-level REST bootstrap refusals.",
            fixture: "tests/fixtures/doctor_detectors/circular_ref_spec/spec.yaml",
            signal: DetectorSignal {
                kind: "process_refusal",
                code: "E_OPENAPI_REF",
                sqlstate: None,
                json_path: Some("$.refusal.code"),
            },
            fix_available: false,
            required_before_fix: fix_requirements(),
        },
        DetectorSpec {
            id: "nested_path_spec",
            failure_mode: "Nested REST resource paths must be classified as unsupported route refusals with startup warnings.",
            fixture: "tests/fixtures/doctor_detectors/nested_path_spec/spec.yaml",
            signal: DetectorSignal {
                kind: "route_refusal",
                code: "unsupported_shape",
                sqlstate: Some("0A000"),
                json_path: Some("$.routes[*].kind=refusal"),
            },
            fix_available: false,
            required_before_fix: fix_requirements(),
        },
    ]
}

fn fix_requirements() -> Vec<&'static str> {
    vec![
        "detector_fixture",
        "verbatim_backup",
        "explicit_inverse",
        "regression_fixture",
    ]
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
        Recommendation {
            id: "rest_protocol_detector_fixtures",
            priority: "p3",
            action: "Keep REST detector fixtures aligned with malformed OpenAPI specs, circular refs, and nested-path refusals.",
            reason: "REST support must be explicit about unsupported spec and route shapes before the HTTP listener broadens.",
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
        format!("config_root: {}", report.config_footprint.canonical_root),
        String::from("REST protocol: twinning rest --spec <openapi.yaml> [OPTIONS]"),
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
        format!("config_root: {}", report.config_footprint.canonical_root),
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
        "  twinning rest --spec <openapi.yaml> --json",
        "  twinning mcp --server <COMMAND> --json",
        "  twinning snowflake --schema <FILE> --json",
        "notes:",
        "  doctor mode does not read schema, snapshot, or verify files",
        "  doctor mode does not bind sockets, run child commands, or write artifacts",
        "  doctor --fix is intentionally unavailable until detector fixtures, backups, inverses, and undo tests exist",
        "",
    ]
    .join("\n")
}
