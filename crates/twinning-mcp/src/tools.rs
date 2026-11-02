//! Stable tool descriptors and handlers for the declared Postgres subset.

use std::env;
use std::fs::{self, DirBuilder, File};
use std::io::Read;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value, json};

use crate::{
    argv::SeedContractArgv,
    doctor,
    errors::ToolError,
    locate,
    paths::{
        Workdir, reject_nonlocal_refs, resolve_input_file, resolve_output_file, validate_restricted,
    },
    registry::{
        DEFAULT_MAX_TWINS, Registry, TwinKind, TwinRecord, TwinStatus, connection_string,
        parse_ttl_minutes,
    },
    report, stale,
    supervisor::{Supervisor, SupervisorConfig, TwinStartRequest},
};

#[derive(Debug, Clone, Default)]
pub struct ToolServiceConfig {
    pub workdir: Option<PathBuf>,
    pub twinning_bin: Option<PathBuf>,
    pub restricted: bool,
    pub max_twins: Option<usize>,
}

pub struct ToolService {
    config: ToolServiceConfig,
    workdir: Result<Workdir, ToolError>,
    env_twinning_bin: Option<PathBuf>,
    state_root: PathBuf,
    supervisor: Option<Supervisor>,
}

impl ToolService {
    pub fn new(mut config: ToolServiceConfig) -> Self {
        config.restricted |= env::var("TWINNING_MCP_RESTRICTED").is_ok_and(|value| value == "1");
        let launch_dir = env::current_dir().unwrap_or_else(|_| PathBuf::from("/"));
        let home = env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| launch_dir.clone());
        let env_workdir = env::var_os("TWINNING_MCP_WORKDIR").map(PathBuf::from);
        let workdir = Workdir::resolve(
            config.workdir.as_deref(),
            env_workdir.as_deref(),
            &launch_dir,
            &home,
        );
        let env_twinning_bin = env::var_os("TWINNING_BIN").map(PathBuf::from);
        let state_root = env::temp_dir()
            .join("twinning-mcp")
            .join(std::process::id().to_string());
        Self {
            config,
            workdir,
            env_twinning_bin,
            state_root,
            supervisor: None,
        }
    }

    pub fn handle(&mut self, name: &str, arguments: &Value) -> Result<Value, ToolError> {
        let object = arguments.as_object().ok_or_else(|| ToolError {
            code: "E_ARGUMENT_INVALID",
            message: "arguments: expected an object".into(),
            hint: "Pass an object in the arguments field",
        })?;
        match name {
            "twin_start" => self.twin_start(object),
            "twin_pair_proof" => self.twin_pair_proof(object),
            "twin_list" => self.twin_list(object),
            "twin_stop" => self.twin_stop(object),
            "twin_report" => self.twin_report(object),
            "twin_seed_contract" => self.twin_seed_contract(object),
            "twin_doctor" => self.twin_doctor(object),
            _ => Err(ToolError {
                code: "E_ARGUMENT_INVALID",
                message: format!("tool: unknown tool `{name}`"),
                hint: "Use tools/list to choose a supported tool",
            }),
        }
    }

    pub fn workdir_log_fields(&self) -> [(&'static str, String); 2] {
        match &self.workdir {
            Ok(workdir) => [
                ("path", workdir.path.display().to_string()),
                ("safe", (!workdir.unsafe_default).to_string()),
            ],
            Err(error) => [
                ("path", "<unavailable>".into()),
                ("safe", error.code.to_owned()),
            ],
        }
    }

    pub fn shutdown(&self) {
        if let Some(supervisor) = &self.supervisor {
            supervisor.shutdown_all();
        }
    }

    fn twin_start(&mut self, arguments: &Map<String, Value>) -> Result<Value, ToolError> {
        reject_unknown(
            arguments,
            &[
                "kind",
                "schema_path",
                "spec_path",
                "verify_path",
                "seed_path",
                "restore_path",
                "snapshot_path",
                "declaration_path",
                "auth_mode",
                "server_variables",
                "capture_refused_statements",
                "ttl_minutes",
            ],
        )?;
        let kind = required_string(arguments, "kind")?;
        let kind = match kind {
            "postgres" => TwinKind::Postgres,
            "rest" => TwinKind::Rest,
            other => return Err(kind_unsupported(other)),
        };
        let ttl = match arguments.get("ttl_minutes") {
            Some(value) => parse_ttl_minutes(value)?,
            None => 60,
        };
        let workdir = self.safe_workdir()?.path.clone();
        let capture_refused_statements = match kind {
            TwinKind::Postgres => {
                optional_bool(arguments, "capture_refused_statements")?.unwrap_or(false)
            }
            TwinKind::Rest => {
                reject_wrong_kind_field(arguments, "capture_refused_statements", "postgres")?;
                false
            }
        };
        let (input, verify, seed, restore, snapshot, declaration, auth_mode, server_variables) =
            match kind {
                TwinKind::Postgres => {
                    for field in ["spec_path", "auth_mode", "server_variables"] {
                        reject_wrong_kind_field(arguments, field, "postgres")?;
                    }
                    let schema_raw = required_string(arguments, "schema_path")?;
                    let verify_raw = optional_string(arguments, "verify_path")?;
                    let seed_raw = optional_string(arguments, "seed_path")?;
                    let restore_raw = optional_string(arguments, "restore_path")?;
                    let snapshot_raw = optional_string(arguments, "snapshot_path")?;
                    let declaration_raw = optional_string(arguments, "declaration_path")?;
                    if self.config.restricted
                        && (snapshot_raw.is_some() || declaration_raw.is_some())
                    {
                        let field = if snapshot_raw.is_some() {
                            "snapshot_path"
                        } else {
                            "declaration_path"
                        };
                        return Err(argument_error(field, "is unavailable in restricted mode"));
                    }
                    validate_restricted(self.config.restricted, verify_raw, seed_raw, restore_raw)?;
                    (
                        resolve_argument_file("schema_path", schema_raw, &workdir)?,
                        verify_raw
                            .map(|path| resolve_argument_file("verify_path", path, &workdir))
                            .transpose()?,
                        seed_raw
                            .map(|path| resolve_argument_file("seed_path", path, &workdir))
                            .transpose()?,
                        restore_raw
                            .map(|path| resolve_argument_file("restore_path", path, &workdir))
                            .transpose()?,
                        snapshot_raw
                            .map(|path| resolve_argument_output("snapshot_path", path, &workdir))
                            .transpose()?,
                        declaration_raw
                            .map(|path| resolve_argument_file("declaration_path", path, &workdir))
                            .transpose()?,
                        None,
                        Vec::new(),
                    )
                }
                TwinKind::Rest => {
                    for field in [
                        "schema_path",
                        "verify_path",
                        "seed_path",
                        "restore_path",
                        "snapshot_path",
                        "declaration_path",
                    ] {
                        reject_wrong_kind_field(arguments, field, "rest")?;
                    }
                    let spec_raw = required_string(arguments, "spec_path")?;
                    let auth_mode = match optional_string(arguments, "auth_mode")? {
                        None | Some("shape") => "shape".to_owned(),
                        Some("bypass") => "bypass".to_owned(),
                        Some(_) => {
                            return Err(argument_error(
                                "auth_mode",
                                "must be shape or bypass for kind rest",
                            ));
                        }
                    };
                    let server_variables =
                        parse_server_variables(arguments.get("server_variables"))?;
                    let spec_path = resolve_argument_file("spec_path", spec_raw, &workdir)?;
                    reject_nonlocal_refs(&spec_path)?;
                    (
                        spec_path,
                        None,
                        None,
                        None,
                        None,
                        None,
                        Some(auth_mode),
                        server_variables,
                    )
                }
            };
        let twinning_bin = self.locate_twinning()?;
        let cap = self.configured_cap()?;

        if self.supervisor.as_ref().is_some_and(|supervisor| {
            supervisor
                .records()
                .iter()
                .filter(|record| record.status.is_live())
                .count()
                >= cap
        }) {
            return Err(cap_error(cap));
        }

        let start_result = {
            let supervisor = self.ensure_supervisor(twinning_bin, cap)?;
            let before = supervisor.records().len();
            let result = supervisor.start_typed(TwinStartRequest {
                workdir: workdir.clone(),
                kind,
                input,
                verify,
                seed,
                restore,
                snapshot: snapshot.clone(),
                declaration: declaration.clone(),
                auth_mode,
                server_variables,
                capture_refused_statements,
                ttl: Duration::from_secs(ttl * 60),
            });
            let failed_record = if result.is_err() && supervisor.records().len() > before {
                supervisor.records().last().cloned()
            } else {
                None
            };
            (result, failed_record)
        };
        let record = match start_result {
            (Ok(record), _) => record,
            (Err(mut error), Some(record)) => {
                self.decorate_start_error(&mut error, &record);
                return Err(error);
            }
            (Err(error), None) => return Err(error),
        };
        let pid = record
            .child_pid
            .ok_or_else(|| internal_error("twin child pid is missing"))?;
        let mut response = json!({
            "twin_id": record.id,
            "kind": kind.as_str(),
            "port": record.port,
            "pid": pid,
            "started_at": record.started_at,
            "expires_at": record.expires_at
        });
        if kind == TwinKind::Postgres {
            response["connection_string"] = Value::String(connection_string(record.port));
            if let Some(path) = snapshot {
                response["snapshot_path"] = Value::String(path.display().to_string());
            }
            if let Some(path) = declaration {
                response["declaration_path"] = Value::String(path.display().to_string());
            }
        } else {
            response["base_url"] = Value::String(format!("http://127.0.0.1:{}", record.port));
        }
        Ok(response)
    }

    fn twin_pair_proof(&mut self, arguments: &Map<String, Value>) -> Result<Value, ToolError> {
        reject_unknown(
            arguments,
            &[
                "left_snapshot_path",
                "right_snapshot_path",
                "queries_path",
                "report_path",
            ],
        )?;
        let workdir = self.safe_workdir()?.path.clone();
        let left_raw = required_string(arguments, "left_snapshot_path")?;
        let right_raw = required_string(arguments, "right_snapshot_path")?;
        let queries_raw = required_string(arguments, "queries_path")?;
        let left = resolve_argument_file("left_snapshot_path", left_raw, &workdir)?;
        let right = resolve_argument_file("right_snapshot_path", right_raw, &workdir)?;
        let queries = resolve_argument_file("queries_path", queries_raw, &workdir)?;
        let report_path = match optional_string(arguments, "report_path")? {
            Some(path) => resolve_argument_output("report_path", path, &workdir)?,
            None => {
                let timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos();
                self.state_root
                    .join("proofs")
                    .join(format!("proof-{}-{timestamp}.json", std::process::id()))
            }
        };
        let twinning_bin = self.locate_twinning()?;
        let timeout = proof_timeout();
        let output = run_twin_pair_proof(
            &twinning_bin,
            &workdir,
            &left,
            &right,
            &queries,
            &report_path,
            timeout,
        )?;
        let report_bytes = fs::read(&report_path).map_err(|error| {
            proof_report_error(
                &report_path,
                &output.stderr,
                format!("report unreadable: {error}"),
            )
        })?;
        let proof: Value = serde_json::from_slice(&report_bytes).map_err(|error| {
            proof_report_error(
                &report_path,
                &output.stderr,
                format!("report JSON invalid: {error}"),
            )
        })?;
        report::twin_pair_proof_response(&proof, &report_path, &output.stderr)
    }

    fn twin_list(&self, arguments: &Map<String, Value>) -> Result<Value, ToolError> {
        reject_unknown(arguments, &[])?;
        self.safe_workdir()?;
        let twins = self
            .supervisor
            .as_ref()
            .map(Supervisor::records)
            .unwrap_or_default()
            .iter()
            .map(|record| {
                json!({
                    "twin_id": record.id,
                    "kind": record.kind.as_str(),
                    "status": status_name(record.status),
                    "port": record.port,
                    "started_at": record.started_at,
                    "expires_at": record.expires_at
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({"twins": twins}))
    }

    fn twin_stop(&mut self, arguments: &Map<String, Value>) -> Result<Value, ToolError> {
        reject_unknown(arguments, &["twin_id"])?;
        self.safe_workdir()?;
        let twin_id = required_string(arguments, "twin_id")?;
        let supervisor = self
            .supervisor
            .as_ref()
            .ok_or_else(|| twin_not_found(twin_id))?;
        let result = supervisor.stop(twin_id)?;
        report::stop_summary(&result)
    }

    fn twin_report(&self, arguments: &Map<String, Value>) -> Result<Value, ToolError> {
        reject_unknown(arguments, &["twin_id"])?;
        self.safe_workdir()?;
        let twin_id = required_string(arguments, "twin_id")?;
        let supervisor = self
            .supervisor
            .as_ref()
            .ok_or_else(|| twin_not_found(twin_id))?;
        let record = supervisor
            .records()
            .into_iter()
            .find(|record| record.id == twin_id)
            .ok_or_else(|| twin_not_found(twin_id))?;
        if record.status.is_live() {
            return Err(ToolError {
                code: "E_REPORT_NOT_READY",
                message: format!("twin {twin_id}: report is written only after stop"),
                hint: "Call twin_stop to finalize the report",
            });
        }
        let full_report =
            report::read_report(&record.report_path, &record.stderr_path, record.kind)?;
        if record.kind == TwinKind::Rest {
            return Ok(report::full_rest_report(&full_report, &record.report_path));
        }
        let trace = report::read_trace(&record.trace_path, &record.stderr_path)?;
        Ok(report::full_report(
            full_report,
            &trace,
            &record.report_path,
        ))
    }

    fn twin_seed_contract(&mut self, arguments: &Map<String, Value>) -> Result<Value, ToolError> {
        reject_unknown(arguments, &["kind", "schema_path"])?;
        let kind = required_string(arguments, "kind")?;
        if kind == "rest" {
            return Err(operation_kind_unsupported("twin_seed_contract", kind));
        }
        if kind != "postgres" {
            return Err(kind_unsupported(kind));
        }
        let schema_raw = required_string(arguments, "schema_path")?;
        let workdir = self.safe_workdir()?;
        let schema = resolve_argument_file("schema_path", schema_raw, &workdir.path)?;
        let twinning_bin = self.locate_twinning()?;
        self.ensure_state_root()?;
        let contract_dir = self.create_contract_dir()?;
        let contract_path = contract_dir.join("seed-contract.jsonl");
        let output = Command::new(&twinning_bin)
            .args(
                SeedContractArgv {
                    schema,
                    output: contract_path.clone(),
                }
                .build(),
            )
            .current_dir(&workdir.path)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .map_err(|error| ToolError {
                code: "E_SEED_CONTRACT_FAILED",
                message: format!(
                    "seed contract export could not start: {error}; stderr_tail=<unavailable>"
                ),
                hint: "Check schema_path and run twinning-mcp doctor",
            })?;
        if !output.status.success() {
            return Err(ToolError {
                code: "E_SEED_CONTRACT_FAILED",
                message: format!(
                    "seed contract export exited with code {}; stderr_tail={}",
                    output.status.code().unwrap_or(-1),
                    tail_bytes(&output.stderr)
                ),
                hint: "Check schema_path and run twinning-mcp doctor",
            });
        }
        let table_count = count_contract_tables(&contract_path)?;
        Ok(json!({
            "contract_path": contract_path,
            "table_count": table_count
        }))
    }

    fn twin_doctor(&self, arguments: &Map<String, Value>) -> Result<Value, ToolError> {
        reject_unknown(arguments, &[])?;
        let cap = self.configured_cap().unwrap_or(DEFAULT_MAX_TWINS);
        let mut result = doctor::diagnose(
            self.config.twinning_bin.as_deref(),
            self.env_twinning_bin.as_deref(),
            &self.workdir,
            cap,
            self.config.restricted,
        );
        result["supported_kinds"] = json!(["postgres", "rest"]);
        Ok(result)
    }

    fn safe_workdir(&self) -> Result<&Workdir, ToolError> {
        let workdir = self.workdir.as_ref().map_err(Clone::clone)?;
        if workdir.unsafe_default {
            return Err(ToolError {
                code: "E_WORKDIR_UNSAFE",
                message: format!(
                    "workdir {} is unsafe when selected by default",
                    workdir.path.display()
                ),
                hint: "Pass --workdir <DIR> to explicitly select this directory",
            });
        }
        Ok(workdir)
    }

    fn locate_twinning(&self) -> Result<PathBuf, ToolError> {
        locate::locate(
            self.config.twinning_bin.as_deref(),
            self.env_twinning_bin.as_deref(),
        )
    }

    fn configured_cap(&self) -> Result<usize, ToolError> {
        match self.config.max_twins {
            Some(0) => Err(argument_error("max_twins", "must be a positive integer")),
            Some(cap) => Ok(cap),
            None => Registry::max_live_from_env(env::var("TWINNING_MCP_MAX_TWINS").ok().as_deref()),
        }
    }

    fn ensure_supervisor(
        &mut self,
        twinning_bin: PathBuf,
        cap: usize,
    ) -> Result<&Supervisor, ToolError> {
        if self.supervisor.is_none() {
            self.supervisor = Some(Supervisor::new(SupervisorConfig {
                twinning_bin,
                state_root: self.state_root.clone(),
                max_twins: cap,
                ready_timeout: None,
                stop_timeout: None,
            })?);
        }
        Ok(self
            .supervisor
            .as_ref()
            .expect("supervisor just initialized"))
    }

    fn ensure_state_root(&self) -> Result<(), ToolError> {
        let mut builder = DirBuilder::new();
        builder.recursive(true).mode(0o700);
        builder
            .create(&self.state_root)
            .map_err(|error| internal_error(&format!("state directory: {error}")))?;
        fs::set_permissions(&self.state_root, fs::Permissions::from_mode(0o700))
            .map_err(|error| internal_error(&format!("state directory permissions: {error}")))?;
        if let Some(parent) = self.state_root.parent() {
            stale::sweep_stale(parent, std::process::id())
                .map_err(|error| internal_error(&format!("stale state sweep: {error}")))?;
        }
        Ok(())
    }

    fn create_contract_dir(&self) -> Result<PathBuf, ToolError> {
        for _ in 0..128 {
            let mut bytes = [0_u8; 3];
            File::open("/dev/urandom")
                .and_then(|mut random| random.read_exact(&mut bytes))
                .map_err(|error| internal_error(&format!("contract id randomness: {error}")))?;
            let name = format!("contract_{:02x}{:02x}{:02x}", bytes[0], bytes[1], bytes[2]);
            let path = self.state_root.join(name);
            let mut builder = DirBuilder::new();
            builder.mode(0o700);
            match builder.create(&path) {
                Ok(()) => return Ok(path),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(internal_error(&format!("contract directory: {error}"))),
            }
        }
        Err(internal_error(
            "contract directory id collision limit reached",
        ))
    }

    fn decorate_start_error(&self, error: &mut ToolError, record: &TwinRecord) {
        if error.code != "E_START_FAILED" {
            return;
        }
        let refusal = fs::read(&record.stdout_path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            .filter(|value| value["version"] == "twinning.v0" && value["outcome"] == "REFUSAL")
            .and_then(|value| value["refusal"]["code"].as_str().map(str::to_owned));
        let stderr = fs::read(&record.stderr_path)
            .map(|bytes| tail_bytes(&bytes))
            .unwrap_or_else(|_| "<unavailable>".into());
        let refusal_text = refusal
            .map(|code| format!(" refusal_code={code};"))
            .unwrap_or_default();
        error.message = format!("{};{} stderr_tail={stderr}", error.message, refusal_text);
    }
}

pub fn tool_definitions() -> Vec<Value> {
    vec![
        json!({
            "name": "twin_doctor",
            "description": "Check the twinning binary and configured work directory for the declared-subset Postgres and spec-based REST twin server.",
            "inputSchema": empty_object_schema(),
            "annotations": { "readOnlyHint": true }
        }),
        json!({
            "name": "twin_list",
            "description": "List retained Postgres and spec-based REST twins managed for the declared client subset.",
            "inputSchema": empty_object_schema(),
            "annotations": { "readOnlyHint": true }
        }),
        json!({
            "name": "twin_pair_proof",
            "description": "Compare declared-subset Postgres-to-Postgres committed-state snapshots by replaying point lookups, filtered scans and aggregate counts from the supplied query file. Join cases are reported as skip. Oracle/TNS fidelity is deferred.",
            "inputSchema": object_schema(
                json!({
                    "left_snapshot_path": { "type": "string", "description": "Path to the left committed-state snapshot inside the workdir." },
                    "right_snapshot_path": { "type": "string", "description": "Path to the right committed-state snapshot inside the workdir." },
                    "queries_path": { "type": "string", "description": "Path to the proof query file inside the workdir." },
                    "report_path": { "type": "string", "description": "Optional proof report output path inside the workdir." }
                }),
                vec!["left_snapshot_path", "right_snapshot_path", "queries_path"]
            ),
            "annotations": { "readOnlyHint": false, "idempotentHint": false }
        }),
        json!({
            "name": "twin_report",
            "description": "Read a bounded report for a stopped declared-subset Postgres or spec-based REST twin. Opt-in refused Postgres SQL text appears only here and can contain SQL literals.",
            "inputSchema": object_schema(
                json!({ "twin_id": { "type": "string", "description": "Twin id returned by twin_start." } }),
                vec!["twin_id"]
            ),
            "annotations": { "readOnlyHint": true }
        }),
        json!({
            "name": "twin_seed_contract",
            "description": "Create a seed contract for a schema supported by the declared Postgres subset.",
            "inputSchema": object_schema(
                json!({
                    "kind": { "type": "string", "enum": ["postgres"] },
                    "schema_path": { "type": "string", "description": "Path to the schema file." }
                }),
                vec!["kind", "schema_path"]
            ),
            "annotations": { "readOnlyHint": false }
        }),
        json!({
            "name": "twin_start",
            "description": "Start a Postgres twin for the declared client subset or a REST twin built from an OpenAPI spec; the REST twin refuses requests the spec does not define. Optional Postgres declaration_path sets catalog identity. Optional snapshot_path is written only when the twin stops. Opt-in refused SQL capture writes text only to the local trace file and twin_report; SQL text can contain literals, and bind values are never captured.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "kind": { "type": "string", "enum": ["postgres", "rest"] },
                    "schema_path": { "type": "string", "description": "Path to a schema within the declared Postgres subset." },
                    "spec_path": { "type": "string", "description": "Path to an OpenAPI YAML or JSON spec inside the workdir; external references are refused." },
                    "verify_path": { "type": "string", "description": "Optional verify artifact path for Postgres." },
                    "seed_path": { "type": "string", "description": "Optional seed file path for Postgres." },
                    "restore_path": { "type": "string", "description": "Optional snapshot path to restore for Postgres." },
                    "snapshot_path": { "type": "string", "description": "Optional Postgres snapshot output path. The snapshot appears only after the twin stops." },
                    "declaration_path": { "type": "string", "description": "Optional Postgres catalog declaration path." },
                    "auth_mode": { "type": "string", "enum": ["shape", "bypass"], "default": "shape", "description": "REST authentication behavior." },
                    "server_variables": { "type": "object", "additionalProperties": { "type": "string" }, "description": "OpenAPI server variables passed as repeated --server-variable options." },
                    "capture_refused_statements": { "type": "boolean", "default": false, "description": "Postgres only. Store refused SQL text in the local trace file and expose bounded samples in twin_report. Text may contain SQL literals; bind values are never captured." },
                    "ttl_minutes": { "type": "integer", "minimum": 1, "maximum": 1440, "default": 60 }
                },
                "required": ["kind"],
                "additionalProperties": false,
                "oneOf": [
                    { "properties": { "kind": { "const": "postgres" } }, "required": ["schema_path"] },
                    { "properties": { "kind": { "const": "rest" } }, "required": ["spec_path"], "not": { "anyOf": [{ "required": ["capture_refused_statements"] }, { "required": ["snapshot_path"] }, { "required": ["declaration_path"] }] } }
                ]
            },
            "annotations": { "readOnlyHint": false, "idempotentHint": false }
        }),
        json!({
            "name": "twin_stop",
            "description": "Stop a managed declared-subset Postgres or REST twin and return its run summary.",
            "inputSchema": object_schema(
                json!({ "twin_id": { "type": "string", "description": "Twin id returned by twin_start." } }),
                vec!["twin_id"]
            ),
            "annotations": { "readOnlyHint": false, "destructiveHint": false }
        }),
    ]
}

pub fn tool_list_result() -> Value {
    json!({ "tools": tool_definitions() })
}

pub fn tool_names() -> Vec<String> {
    tool_definitions()
        .iter()
        .filter_map(|tool| tool["name"].as_str().map(str::to_owned))
        .collect()
}

fn empty_object_schema() -> Value {
    object_schema(json!({}), Vec::new())
}

fn object_schema(properties: Value, required: Vec<&str>) -> Value {
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false
    })
}

fn reject_unknown(arguments: &Map<String, Value>, allowed: &[&str]) -> Result<(), ToolError> {
    if let Some(field) = arguments
        .keys()
        .find(|field| !allowed.contains(&field.as_str()))
    {
        return Err(argument_error(field, "is not an allowed argument"));
    }
    Ok(())
}

fn reject_wrong_kind_field(
    arguments: &Map<String, Value>,
    field: &str,
    allowed_kind: &str,
) -> Result<(), ToolError> {
    if arguments.contains_key(field) {
        let detail = match allowed_kind {
            "postgres" => "is only valid for kind postgres",
            "rest" => "is only valid for kind rest",
            _ => "is not valid for this kind",
        };
        return Err(argument_error(field, detail));
    }
    Ok(())
}

fn parse_server_variables(value: Option<&Value>) -> Result<Vec<(String, String)>, ToolError> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let object = value.as_object().ok_or_else(|| {
        argument_error(
            "server_variables",
            "must be an object of NAME to string values",
        )
    })?;
    let mut variables = Vec::with_capacity(object.len());
    for (name, value) in object {
        if !valid_server_variable_name(name) {
            return Err(argument_error(
                "server_variables",
                "keys must be non-empty names without equals signs or whitespace",
            ));
        }
        let value = value
            .as_str()
            .ok_or_else(|| argument_error("server_variables", "every value must be a string"))?;
        if value.chars().any(char::is_control) {
            return Err(argument_error(
                "server_variables",
                "values must not contain control characters",
            ));
        }
        variables.push((name.clone(), value.to_owned()));
    }
    Ok(variables)
}

fn valid_server_variable_name(name: &str) -> bool {
    let mut bytes = name.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn required_string<'a>(
    arguments: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a str, ToolError> {
    arguments
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| argument_error(field, "is required and must be a string"))
}

fn optional_string<'a>(
    arguments: &'a Map<String, Value>,
    field: &str,
) -> Result<Option<&'a str>, ToolError> {
    arguments
        .get(field)
        .map(|value| {
            value
                .as_str()
                .ok_or_else(|| argument_error(field, "must be a string when provided"))
        })
        .transpose()
}

fn optional_bool(arguments: &Map<String, Value>, field: &str) -> Result<Option<bool>, ToolError> {
    arguments
        .get(field)
        .map(|value| {
            value
                .as_bool()
                .ok_or_else(|| argument_error(field, "must be a boolean when provided"))
        })
        .transpose()
}

fn argument_error(field: &str, detail: &str) -> ToolError {
    let hint = match field {
        "kind" => "Choose kind postgres or rest",
        "schema_path" => "Set schema_path to a file inside the workdir",
        "spec_path" => "Set spec_path to an OpenAPI file inside the workdir",
        "verify_path" => "Set verify_path to a file inside the workdir or omit it",
        "seed_path" => "Set seed_path to a file inside the workdir or omit it",
        "restore_path" => "Set restore_path to a file inside the workdir or omit it",
        "snapshot_path" => "Set snapshot_path to an output file inside the workdir or omit it",
        "declaration_path" => "Set declaration_path to a file inside the workdir or omit it",
        "left_snapshot_path" => "Set left_snapshot_path to a snapshot inside the workdir",
        "right_snapshot_path" => "Set right_snapshot_path to a snapshot inside the workdir",
        "queries_path" => "Set queries_path to a proof query file inside the workdir",
        "report_path" => "Set report_path to an output file inside the workdir or omit it",
        "auth_mode" => "Set auth_mode to shape or bypass for kind rest",
        "server_variables" => "Set server_variables to an object of NAME to string values",
        "capture_refused_statements" => {
            "Set capture_refused_statements to true or false for kind postgres"
        }
        "ttl_minutes" => "Set ttl_minutes to an integer from 1 through 1440",
        "twin_id" => "Set twin_id to an id returned by twin_start",
        "max_twins" => "Set max_twins to a positive integer",
        _ => "Remove the unknown field named in the error message",
    };
    ToolError {
        code: "E_ARGUMENT_INVALID",
        message: format!("{field}: {detail}"),
        hint,
    }
}

fn resolve_argument_file(
    field: &'static str,
    path: &str,
    workdir: &Path,
) -> Result<PathBuf, ToolError> {
    resolve_input_file(path, workdir).map_err(|mut error| {
        error.message = format!("{field}: {}", error.message);
        error.hint = match field {
            "schema_path" => "Check schema_path relative to the workdir",
            "spec_path" => "Check spec_path relative to the workdir",
            "verify_path" => "Check verify_path relative to the workdir",
            "seed_path" => "Check seed_path relative to the workdir",
            "restore_path" => "Check restore_path relative to the workdir",
            "declaration_path" => "Check declaration_path relative to the workdir",
            "left_snapshot_path" => "Check left_snapshot_path relative to the workdir",
            "right_snapshot_path" => "Check right_snapshot_path relative to the workdir",
            "queries_path" => "Check queries_path relative to the workdir",
            _ => "Check the named path relative to the workdir",
        };
        error
    })
}

fn resolve_argument_output(
    field: &'static str,
    path: &str,
    workdir: &Path,
) -> Result<PathBuf, ToolError> {
    resolve_output_file(path, workdir).map_err(|mut error| {
        error.message = format!("{field}: {}", error.message);
        error.hint = match field {
            "snapshot_path" => "Set snapshot_path to a file inside the workdir",
            "report_path" => "Set report_path to a file inside the workdir",
            _ => "Set the named output path to a file inside the workdir",
        };
        error
    })
}

struct ProofOutput {
    stderr: String,
}

fn proof_timeout() -> Duration {
    env::var("TWINNING_MCP_PROOF_TIMEOUT_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|millis| *millis > 0)
        .map(Duration::from_millis)
        .unwrap_or(Duration::from_secs(60))
}

fn run_twin_pair_proof(
    binary: &Path,
    workdir: &Path,
    left: &Path,
    right: &Path,
    queries: &Path,
    report_path: &Path,
    timeout: Duration,
) -> Result<ProofOutput, ToolError> {
    let mut command = Command::new(binary); // ubs:ignore - binary is operator-supplied (flag/env/sibling), never a tool argument
    command
        .arg("--json")
        .arg("proof")
        .arg("twin-pair")
        .arg("--left")
        .arg(left)
        .arg("--right")
        .arg(right)
        .arg("--queries")
        .arg(queries)
        .arg("--report")
        .arg(report_path)
        .current_dir(workdir)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .process_group(0);
    let mut child = command.spawn().map_err(|error| ToolError {
        code: "E_INTERNAL",
        message: format!("twin-pair proof could not start: {error}; stderr_tail=<empty>"),
        hint: "Check the twinning binary and run twinning-mcp doctor",
    })?;
    let stderr = child.stderr.take().expect("proof stderr is piped");
    let stderr_reader = std::thread::spawn(move || read_stream_tail(stderr));
    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(10));
            }
            Ok(None) => {
                // SAFETY: the child started a fresh process group owned by this tool call.
                let _ = unsafe { libc::killpg(child.id() as libc::pid_t, libc::SIGKILL) };
                let _ = child.wait();
                let stderr = stderr_reader
                    .join()
                    .unwrap_or_else(|_| "<unavailable>".into());
                return Err(ToolError {
                    code: "E_INTERNAL",
                    message: format!(
                        "twin-pair proof timed out after {} ms; stderr_tail={}",
                        timeout.as_millis(),
                        stderr_tail_text(&stderr)
                    ),
                    hint: "Reduce the proof query set or raise TWINNING_MCP_PROOF_TIMEOUT_MS",
                });
            }
            Err(error) => {
                // SAFETY: the child started a fresh process group owned by this tool call.
                let _ = unsafe { libc::killpg(child.id() as libc::pid_t, libc::SIGKILL) };
                let _ = child.wait();
                let stderr = stderr_reader
                    .join()
                    .unwrap_or_else(|_| "<unavailable>".into());
                return Err(ToolError {
                    code: "E_INTERNAL",
                    message: format!(
                        "twin-pair proof wait failed: {error}; stderr_tail={}",
                        stderr_tail_text(&stderr)
                    ),
                    hint: "Check the twinning binary and run twinning-mcp doctor",
                });
            }
        }
    };
    let stderr = stderr_reader
        .join()
        .unwrap_or_else(|_| "<unavailable>".into());
    if !status.success() {
        return Err(ToolError {
            code: "E_INTERNAL",
            message: format!(
                "twin-pair proof exited with {}; stderr_tail={}",
                status
                    .code()
                    .map_or_else(|| "signal".into(), |code| code.to_string()),
                stderr_tail_text(&stderr)
            ),
            hint: "Check the proof inputs and run twinning-mcp doctor",
        });
    }
    Ok(ProofOutput { stderr })
}

fn read_stream_tail(mut reader: impl Read) -> String {
    const LIMIT: usize = 4096;
    let mut retained = Vec::with_capacity(LIMIT);
    let mut buffer = [0_u8; 1024];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) | Err(_) => break,
            Ok(length) => {
                retained.extend_from_slice(&buffer[..length]);
                if retained.len() > LIMIT {
                    retained.drain(..retained.len() - LIMIT);
                }
            }
        }
    }
    String::from_utf8_lossy(&retained).into_owned()
}

fn stderr_tail_text(stderr: &str) -> String {
    let sanitized = stderr
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();
    if sanitized.trim().is_empty() {
        "<empty>".into()
    } else {
        sanitized
    }
}

fn proof_report_error(path: &Path, stderr: &str, detail: String) -> ToolError {
    ToolError {
        code: "E_REPORT_UNREADABLE",
        message: format!(
            "twin-pair proof report unreadable at {}: {detail}; stderr_tail={}",
            path.display(),
            stderr_tail_text(stderr)
        ),
        hint: "Check the proof report and run twinning-mcp doctor",
    }
}

fn kind_unsupported(kind: &str) -> ToolError {
    ToolError {
        code: "E_KIND_UNSUPPORTED",
        message: format!("kind: `{kind}` is not available"),
        hint: "Choose kind postgres or rest",
    }
}

fn operation_kind_unsupported(operation: &str, kind: &str) -> ToolError {
    ToolError {
        code: "E_KIND_UNSUPPORTED",
        message: format!("{operation}: kind `{kind}` is not supported"),
        hint: "Use kind postgres for twin_seed_contract",
    }
}

fn internal_error(message: &str) -> ToolError {
    ToolError {
        code: "E_INTERNAL",
        message: message.to_owned(),
        hint: "Run twinning-mcp doctor",
    }
}

fn cap_error(cap: usize) -> ToolError {
    ToolError {
        code: "E_TWIN_CAP_REACHED",
        message: format!("twin cap: at most {cap} twins may run"),
        hint: "Call twin_list, then twin_stop on one twin",
    }
}

fn twin_not_found(twin_id: &str) -> ToolError {
    ToolError {
        code: "E_TWIN_NOT_FOUND",
        message: format!("twin_id: `{twin_id}` was not found"),
        hint: "Call twin_list for a retained twin id",
    }
}

fn status_name(status: TwinStatus) -> &'static str {
    match status {
        TwinStatus::Starting | TwinStatus::Running | TwinStatus::Stopping => "running",
        TwinStatus::Stopped => "stopped",
        TwinStatus::Expired => "expired",
    }
}

fn count_contract_tables(path: &Path) -> Result<usize, ToolError> {
    let contents = fs::read_to_string(path).map_err(|error| ToolError {
        code: "E_SEED_CONTRACT_FAILED",
        message: format!("seed contract output unreadable: {error}; stderr_tail=<empty>"),
        hint: "Check schema_path and run twinning-mcp doctor",
    })?;
    let count = contents
        .lines()
        .filter(|line| {
            serde_json::from_str::<Value>(line)
                .ok()
                .is_some_and(|record| record["target_kind"] == "relation")
        })
        .count();
    Ok(count)
}

fn tail_bytes(bytes: &[u8]) -> String {
    const MAX_TAIL_BYTES: usize = 2048;
    let start = bytes.len().saturating_sub(MAX_TAIL_BYTES);
    let tail = String::from_utf8_lossy(&bytes[start..])
        .lines()
        .collect::<Vec<_>>()
        .join(" | ");
    let sanitized = tail
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "<empty>".into()
    } else {
        sanitized
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::{ToolService, ToolServiceConfig, tool_definitions, tool_names};

    #[test]
    fn tool_names_follow_sorted_tool_descriptors_and_claim_the_declared_subset() {
        let names = tool_names();
        assert!(names.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(tool_definitions().iter().all(|tool| {
            tool["inputSchema"]["additionalProperties"] == false
                && tool["description"].as_str().unwrap().contains("declared")
        }));
    }

    #[test]
    fn u03_all_tools_reject_missing_wrong_type_and_extra_arguments_with_hints() {
        let temp = tempfile::tempdir().unwrap();
        let mut service = ToolService::new(ToolServiceConfig {
            workdir: Some(temp.path().to_path_buf()),
            ..ToolServiceConfig::default()
        });

        assert_argument_error(
            &mut service,
            "twin_start",
            json!({"schema_path":"schema.sql"}),
            "kind",
        );
        assert_argument_error(
            &mut service,
            "twin_start",
            json!({"kind":7,"schema_path":"schema.sql"}),
            "kind",
        );
        assert_argument_error(
            &mut service,
            "twin_start",
            json!({"kind":"postgres"}),
            "schema_path",
        );
        assert_argument_error(
            &mut service,
            "twin_start",
            json!({"kind":"postgres","schema_path":"schema.sql","extra":true}),
            "extra",
        );

        assert_argument_error(&mut service, "twin_list", json!({"extra":true}), "extra");

        assert_argument_error(&mut service, "twin_stop", json!({}), "twin_id");
        assert_argument_error(&mut service, "twin_stop", json!({"twin_id":1}), "twin_id");
        assert_argument_error(
            &mut service,
            "twin_stop",
            json!({"twin_id":"tw_abcdef","extra":true}),
            "extra",
        );

        assert_argument_error(&mut service, "twin_report", json!({}), "twin_id");
        assert_argument_error(
            &mut service,
            "twin_report",
            json!({"twin_id":[]}),
            "twin_id",
        );
        assert_argument_error(
            &mut service,
            "twin_report",
            json!({"twin_id":"tw_abcdef","extra":true}),
            "extra",
        );

        assert_argument_error(&mut service, "twin_seed_contract", json!({}), "kind");
        assert_argument_error(
            &mut service,
            "twin_seed_contract",
            json!({"kind":7,"schema_path":"schema.sql"}),
            "kind",
        );
        assert_argument_error(
            &mut service,
            "twin_seed_contract",
            json!({"kind":"postgres"}),
            "schema_path",
        );
        assert_argument_error(
            &mut service,
            "twin_seed_contract",
            json!({"kind":"postgres","schema_path":"schema.sql","extra":true}),
            "extra",
        );

        assert_argument_error(&mut service, "twin_doctor", json!({"extra":true}), "extra");
    }

    fn assert_argument_error(service: &mut ToolService, tool: &str, arguments: Value, field: &str) {
        let error = service
            .handle(tool, &arguments)
            .expect_err("invalid arguments are refused");
        assert_eq!("E_ARGUMENT_INVALID", error.code, "{tool} {arguments}");
        assert!(
            error.message.contains(field),
            "{tool} {arguments}: {error:?}"
        );
        assert!(
            !error.hint.trim().is_empty(),
            "{tool} {arguments}: {error:?}"
        );
    }
}
