//! Stable tool descriptors and handlers for the declared Postgres subset.

use std::env;
use std::fs::{self, DirBuilder, File};
use std::io::Read;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use serde_json::{Map, Value, json};

use crate::{
    argv::SeedContractArgv,
    doctor,
    errors::ToolError,
    locate,
    paths::{Workdir, resolve_input_file, validate_restricted},
    registry::{
        DEFAULT_MAX_TWINS, Registry, TwinRecord, TwinStatus, connection_string, parse_ttl_minutes,
    },
    report, stale,
    supervisor::{StartRequest, Supervisor, SupervisorConfig},
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
                "verify_path",
                "seed_path",
                "restore_path",
                "ttl_minutes",
            ],
        )?;
        let kind = required_string(arguments, "kind")?;
        let schema_raw = required_string(arguments, "schema_path")?;
        let verify_raw = optional_string(arguments, "verify_path")?;
        let seed_raw = optional_string(arguments, "seed_path")?;
        let restore_raw = optional_string(arguments, "restore_path")?;
        let ttl = match arguments.get("ttl_minutes") {
            Some(value) => parse_ttl_minutes(value)?,
            None => 60,
        };

        if kind != "postgres" {
            return Err(kind_unsupported(kind));
        }
        let workdir = self.safe_workdir()?.path.clone();
        validate_restricted(self.config.restricted, verify_raw, seed_raw, restore_raw)?;
        let schema = resolve_argument_file("schema_path", schema_raw, &workdir)?;
        let verify = verify_raw
            .map(|path| resolve_argument_file("verify_path", path, &workdir))
            .transpose()?;
        let seed = seed_raw
            .map(|path| resolve_argument_file("seed_path", path, &workdir))
            .transpose()?;
        let restore = restore_raw
            .map(|path| resolve_argument_file("restore_path", path, &workdir))
            .transpose()?;
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
            let result = supervisor.start(StartRequest {
                workdir: workdir.clone(),
                schema,
                verify,
                seed,
                restore,
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
        Ok(json!({
            "twin_id": record.id,
            "kind": "postgres",
            "port": record.port,
            "pid": pid,
            "connection_string": connection_string(record.port),
            "started_at": record.started_at,
            "expires_at": record.expires_at
        }))
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
                    "kind": "postgres",
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
        let full_report = report::read_report(&record.report_path, &record.stderr_path)?;
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
        let schema_raw = required_string(arguments, "schema_path")?;
        if kind != "postgres" {
            return Err(kind_unsupported(kind));
        }
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
        Ok(doctor::diagnose(
            self.config.twinning_bin.as_deref(),
            self.env_twinning_bin.as_deref(),
            &self.workdir,
            cap,
            self.config.restricted,
        ))
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
            "description": "Check the twinning binary and the configured work directory for this declared-subset Postgres twin server.",
            "inputSchema": empty_object_schema(),
            "annotations": { "readOnlyHint": true }
        }),
        json!({
            "name": "twin_list",
            "description": "List retained Postgres twins managed for the declared client subset.",
            "inputSchema": empty_object_schema(),
            "annotations": { "readOnlyHint": true }
        }),
        json!({
            "name": "twin_report",
            "description": "Read the report for a stopped declared-subset Postgres twin.",
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
            "description": "Start a disposable Postgres twin for the manifest-backed declared client subset from a schema file.",
            "inputSchema": object_schema(
                json!({
                    "kind": { "type": "string", "enum": ["postgres"] },
                    "schema_path": { "type": "string", "description": "Path to a schema within the declared Postgres subset." },
                    "verify_path": { "type": "string", "description": "Optional verify artifact path." },
                    "seed_path": { "type": "string", "description": "Optional seed file path." },
                    "restore_path": { "type": "string", "description": "Optional snapshot path to restore." },
                    "ttl_minutes": { "type": "integer", "minimum": 1, "maximum": 1440, "default": 60 }
                }),
                vec!["kind", "schema_path"]
            ),
            "annotations": { "readOnlyHint": false, "idempotentHint": false }
        }),
        json!({
            "name": "twin_stop",
            "description": "Stop a managed declared-subset Postgres twin and return its run summary.",
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

fn argument_error(field: &str, detail: &str) -> ToolError {
    let hint = match field {
        "kind" => "Set kind to postgres",
        "schema_path" => "Set schema_path to a file inside the workdir",
        "verify_path" => "Set verify_path to a file inside the workdir or omit it",
        "seed_path" => "Set seed_path to a file inside the workdir or omit it",
        "restore_path" => "Set restore_path to a file inside the workdir or omit it",
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
            "verify_path" => "Check verify_path relative to the workdir",
            "seed_path" => "Check seed_path relative to the workdir",
            "restore_path" => "Check restore_path relative to the workdir",
            _ => "Check the named path relative to the workdir",
        };
        error
    })
}

fn kind_unsupported(kind: &str) -> ToolError {
    ToolError {
        code: "E_KIND_UNSUPPORTED",
        message: format!("kind: `{kind}` is not available"),
        hint: "Only postgres is available in v0",
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
