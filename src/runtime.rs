use std::{path::Path, time::Duration};

use sha2::{Digest, Sha256};

use crate::{
    backend::BaseSnapshotBackend,
    catalog::{self, Catalog},
    cli::Engine,
    config::TwinConfig,
    kernel::storage::TableStorage,
    protocol::postgres::listener::{PgwireListener, ShutdownHook},
    refusal,
    refusal::RefusalResult,
    report::{LiveVerifyArtifact, RatioMap, RunReport, VerifyArtifactReport},
    snapshot,
};

mod final_artifacts;
pub mod run_child;

use final_artifacts::FinalArtifactEmitter;

const RUN_CHILD_TIMEOUT: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Execution {
    pub exit_code: u8,
    pub stdout: String,
}

#[derive(Debug, Clone)]
struct BootstrapState {
    schema_source: String,
    schema_hash: String,
    verify_artifact: Option<VerifyArtifactReport>,
    verify_report: Option<serde_json::Value>,
    live_verify_artifact: Option<LiveVerifyArtifact>,
    catalog: Catalog,
    restored_from: Option<String>,
    restored_snapshot_hash: Option<String>,
    null_rates: Option<RatioMap>,
    fk_coverage: Option<RatioMap>,
    warnings: Vec<String>,
}

pub fn execute(config: TwinConfig) -> Result<Execution, Box<dyn std::error::Error>> {
    let rendered = match execute_inner(&config) {
        Ok(execution) => execution,
        Err(refusal) => Execution {
            exit_code: 2,
            stdout: refusal.render(config.json)?,
        },
    };

    Ok(rendered)
}

fn execute_inner(config: &TwinConfig) -> RefusalResult<Execution> {
    if config.engine != Engine::Postgres {
        return Err(Box::new(refusal::engine_unimplemented(config.engine)));
    }

    let live_mode = config.run_command.is_some();

    let mut state = if let Some(restore_path) = &config.restore_path {
        restore_state(config, restore_path, !live_mode, !live_mode)?
    } else {
        load_state_from_schema(config, !live_mode)?
    };

    if live_mode
        && let Some(verify_artifact) = load_live_verify_artifact(config.verify_path.as_deref())?
    {
        state.verify_artifact = Some(verify_artifact.report.clone());

        if verify_artifact.batch_only_rule.is_some() {
            return Err(Box::new(refusal::verify_batch_only_rule(&verify_artifact)));
        }

        state.live_verify_artifact = Some(verify_artifact);
    }

    preflight_output_targets(config)?;

    if live_mode {
        return execute_run_mode(config, &state);
    }

    let mut emitter = FinalArtifactEmitter::from_config(config);
    let finalized = emitter.emit_once(&state, None, None)?;

    let stdout = if config.json {
        finalized
            .report
            .render_json()
            .map_err(|error| Box::new(refusal::serialization(error.to_string())))?
    } else {
        finalized.report.render_human()
    };

    Ok(Execution {
        exit_code: execution_exit_domain(None, false),
        stdout,
    })
}

fn execute_run_mode(config: &TwinConfig, state: &BootstrapState) -> RefusalResult<Execution> {
    let shutdown = ShutdownHook::install().map_err(|error| {
        Box::new(refusal::runtime_io(
            "shutdown_hook_install",
            error.to_string(),
        ))
    })?;
    let listener = PgwireListener::bind(&config.host, config.port).map_err(|error| {
        Box::new(refusal::runtime_io(
            "listener_bind",
            format!("{}:{}: {error}", config.host, config.port),
        ))
    })?;
    let bound_port = listener
        .local_addr()
        .map_err(|error| {
            Box::new(refusal::runtime_io(
                "listener_local_addr",
                error.to_string(),
            ))
        })?
        .port();

    let server_shutdown = shutdown.clone();
    let listener_thread =
        std::thread::spawn(move || listener.accept_until_shutdown("run-once", &server_shutdown));

    let command = config
        .run_command
        .as_deref()
        .expect("live mode should include a child command");
    let child_outcome = run_child::orchestrate(command, RUN_CHILD_TIMEOUT).map_err(|error| {
        Box::new(refusal::runtime_io(
            "child_orchestration",
            error.to_string(),
        ))
    })?;

    shutdown.request_shutdown();
    let listener_result = listener_thread.join().map_err(|_| {
        Box::new(refusal::runtime_io(
            "listener_join",
            "pgwire accept loop panicked",
        ))
    })?;
    listener_result.map_err(|error| {
        Box::new(refusal::runtime_io(
            "listener_accept_loop",
            error.to_string(),
        ))
    })?;

    let run = RunReport {
        command: child_outcome.command,
        exit_code: child_outcome.exit_code,
        signal: child_outcome.signal,
        timed_out: child_outcome.timed_out,
    };
    let committed_tables = committed_tables_for_live_run(config, state)?;
    let mut emitter = FinalArtifactEmitter::new(
        config.engine,
        config.host.clone(),
        bound_port,
        config.report_path.clone(),
        config.snapshot_path.clone(),
    );
    let finalized = emitter.emit_once(state, Some(committed_tables), Some(run.clone()))?;
    let verify_failed = verify_failed(finalized.report.verify.as_ref());

    let stdout = if config.json {
        finalized
            .report
            .render_json()
            .map_err(|error| Box::new(refusal::serialization(error.to_string())))?
    } else {
        finalized.report.render_human()
    };

    Ok(Execution {
        exit_code: execution_exit_domain(Some(&run), verify_failed),
        stdout,
    })
}

fn load_state_from_schema(
    config: &TwinConfig,
    load_bootstrap_verify_artifact: bool,
) -> RefusalResult<BootstrapState> {
    let schema_path = config
        .schema_path
        .as_ref()
        .ok_or_else(|| Box::new(refusal::missing_bootstrap_source(config.engine)))?;
    let schema_bytes = std::fs::read(schema_path)
        .map_err(|error| Box::new(refusal::io_read(schema_path, &error)))?;
    let schema_text = String::from_utf8(schema_bytes.clone())
        .map_err(|error| Box::new(refusal::schema_parse(schema_path, error.to_string())))?;
    let catalog = catalog::parse_postgres_schema(&schema_text)
        .map_err(|error| Box::new(refusal::schema_parse(schema_path, error.to_string())))?;

    Ok(BootstrapState {
        schema_source: path_display(schema_path),
        schema_hash: sha256_prefixed(&schema_bytes),
        verify_artifact: if load_bootstrap_verify_artifact {
            load_verify_artifact(config.verify_path.as_deref())?
        } else {
            None
        },
        verify_report: None,
        live_verify_artifact: None,
        catalog,
        restored_from: None,
        restored_snapshot_hash: None,
        null_rates: None,
        fk_coverage: None,
        warnings: Vec::new(),
    })
}

fn restore_state(
    config: &TwinConfig,
    restore_path: &Path,
    load_bootstrap_verify_artifact: bool,
    inherit_snapshot_verify_artifact: bool,
) -> RefusalResult<BootstrapState> {
    let snapshot = snapshot::read_snapshot(restore_path)?;
    let restored_snapshot_hash = snapshot.snapshot_hash.clone();
    let verify_artifact = if load_bootstrap_verify_artifact {
        match config.verify_path.as_deref() {
            Some(path) => load_verify_artifact(Some(path))?,
            None => snapshot.verify_artifact.clone(),
        }
    } else if inherit_snapshot_verify_artifact {
        snapshot.verify_artifact.clone()
    } else {
        None
    };

    Ok(BootstrapState {
        schema_source: snapshot.schema_source,
        schema_hash: snapshot.schema_hash,
        verify_artifact,
        verify_report: None,
        live_verify_artifact: None,
        catalog: snapshot.catalog,
        restored_from: Some(path_display(restore_path)),
        restored_snapshot_hash: Some(restored_snapshot_hash),
        null_rates: None,
        fk_coverage: None,
        warnings: Vec::new(),
    })
}

fn load_live_verify_artifact(path: Option<&Path>) -> RefusalResult<Option<LiveVerifyArtifact>> {
    let Some(path) = path else {
        return Ok(None);
    };

    let bytes = std::fs::read(path).map_err(|error| Box::new(refusal::io_read(path, &error)))?;
    let artifact = LiveVerifyArtifact::parse(path_display(path), &bytes)
        .map_err(|error| Box::new(refusal::verify_artifact_parse(path, error)))?;

    Ok(Some(artifact))
}

fn load_verify_artifact(path: Option<&Path>) -> RefusalResult<Option<VerifyArtifactReport>> {
    let Some(path) = path else {
        return Ok(None);
    };

    let bytes = std::fs::read(path).map_err(|error| Box::new(refusal::io_read(path, &error)))?;
    let value: serde_json::Value = serde_json::from_slice(&bytes)
        .map_err(|error| Box::new(refusal::verify_artifact_parse(path, error.to_string())))?;
    let loaded = count_verify_rules(&value);

    Ok(Some(VerifyArtifactReport {
        source: path_display(path),
        hash: sha256_prefixed(&bytes),
        loaded,
    }))
}

fn count_verify_rules(value: &serde_json::Value) -> usize {
    match value {
        serde_json::Value::Null => 0,
        serde_json::Value::Array(items) => items.len(),
        serde_json::Value::Object(object) => object
            .get("rules")
            .and_then(|rules| rules.as_array().map(std::vec::Vec::len))
            .unwrap_or(1),
        _ => 1,
    }
}

fn preflight_output_targets(config: &TwinConfig) -> RefusalResult<()> {
    if let Some(report_path) = &config.report_path {
        preflight_output_target(report_path)?;
    }
    if let Some(snapshot_path) = &config.snapshot_path {
        preflight_output_target(snapshot_path)?;
    }
    Ok(())
}

fn preflight_output_target(path: &Path) -> RefusalResult<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .map_err(|error| Box::new(refusal::io_write(path, &error)))?;
    }

    if path.exists() {
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(|error| Box::new(refusal::io_write(path, &error)))?;
        return Ok(());
    }

    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_stem = path
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("output");

    for attempt in 0..8 {
        let probe_path = parent.join(format!(
            ".{file_stem}.twinning-preflight-{}-{attempt}",
            std::process::id()
        ));
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&probe_path)
        {
            Ok(file) => {
                drop(file);
                std::fs::remove_file(probe_path)
                    .map_err(|error| Box::new(refusal::io_write(path, &error)))?;
                return Ok(());
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(Box::new(refusal::io_write(path, &error))),
        }
    }

    Err(Box::new(refusal::io_write(
        path,
        &std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "failed to create unique preflight probe file",
        ),
    )))
}

fn write_json<T: serde::Serialize>(path: &Path, value: &T) -> RefusalResult<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .map_err(|error| Box::new(refusal::io_write(path, &error)))?;
    }

    let rendered = serde_json::to_string_pretty(value)
        .map_err(|error| Box::new(refusal::serialization(error.to_string())))?;
    std::fs::write(path, format!("{rendered}\n"))
        .map_err(|error| Box::new(refusal::io_write(path, &error)))
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("sha256:{:x}", digest.finalize())
}

fn path_display(path: &Path) -> String {
    path.display().to_string()
}

fn committed_tables_for_live_run(
    config: &TwinConfig,
    state: &BootstrapState,
) -> RefusalResult<Vec<TableStorage>> {
    if let Some(restore_path) = &config.restore_path {
        let snapshot = snapshot::read_snapshot(restore_path)?;
        return snapshot::restore::restore_committed_tables(&snapshot);
    }

    let committed_tables = state
        .catalog
        .tables
        .iter()
        .map(|table| {
            TableStorage::new(table)
                .map_err(|error| Box::new(refusal::serialization(error.to_string())))
        })
        .collect::<RefusalResult<Vec<_>>>()?;

    BaseSnapshotBackend::new(committed_tables.clone())
        .map_err(|error| Box::new(refusal::serialization(error.to_string())))?;

    Ok(committed_tables)
}

fn verify_failed(verify: Option<&serde_json::Value>) -> bool {
    verify
        .and_then(|verify| verify.get("outcome"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|outcome| outcome == "FAIL")
}

fn execution_exit_domain(_run: Option<&RunReport>, verify_failed: bool) -> u8 {
    if verify_failed {
        return 1;
    }

    0
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, fs, net::TcpListener};

    use serde_json::json;
    use tempfile::tempdir;

    use crate::{
        catalog,
        cli::Engine,
        config::TwinConfig,
        snapshot::{self, TwinSnapshot},
    };

    use crate::report::RunReport;

    use super::{execute, execution_exit_domain};

    fn write_schema(path: &std::path::Path) {
        fs::write(
            path,
            "CREATE TABLE public.deals (deal_id TEXT PRIMARY KEY, tenant_id TEXT);",
        )
        .expect("write schema");
    }

    fn write_batch_only_live_verify(path: &std::path::Path) {
        fs::write(
            path,
            r#"{
              "version": "verify.constraint.v1",
              "constraint_set_id": "fixtures.query_rules.orphan_rows",
              "bindings": [
                { "name": "property", "kind": "relation" },
                { "name": "tenants", "kind": "relation" }
              ],
              "rules": [
                {
                  "id": "ORPHAN_PROPERTY_TENANT",
                  "severity": "error",
                  "portability": "batch_only",
                  "check": {
                    "op": "query_zero_rows",
                    "bindings": ["property", "tenants"],
                    "query": "SELECT 1"
                  }
                }
              ]
            }"#,
        )
        .expect("write batch-only verify");
    }

    #[test]
    fn bootstrap_writes_report_and_snapshot() {
        let tempdir = tempdir().expect("tempdir");
        let schema_path = tempdir.path().join("schema.sql");
        let verify_path = tempdir.path().join("verify.json");
        let report_path = tempdir.path().join("out").join("bootstrap.json");
        let snapshot_path = tempdir.path().join("out").join("bootstrap.twin");

        fs::write(
            &schema_path,
            r#"
            CREATE TABLE public.deals (
                deal_id TEXT PRIMARY KEY,
                deal_name TEXT NOT NULL
            );
        "#,
        )
        .expect("schema write");
        fs::write(&verify_path, r#"{ "rules": [{"id": "DEAL_EXISTS"}] }"#).expect("verify write");

        let config = TwinConfig {
            engine: Engine::Postgres,
            host: "127.0.0.1".to_owned(),
            port: 5432,
            schema_path: Some(schema_path.clone()),
            verify_path: Some(verify_path.clone()),
            run_command: None,
            report_path: Some(report_path.clone()),
            snapshot_path: Some(snapshot_path.clone()),
            restore_path: None,
            json: true,
        };

        let execution = execute(config).expect("execution");
        assert_eq!(execution.exit_code, 0);
        assert!(report_path.exists());
        assert!(snapshot_path.exists());

        let json: serde_json::Value = serde_json::from_str(&execution.stdout).expect("json");
        assert_eq!(json["outcome"], "READY");
        assert_eq!(json["schema"]["table_count"], 1);
        assert_eq!(json["verify_artifact"]["loaded"], 1);
        assert!(json.get("verify").is_none());
    }

    #[test]
    fn run_mode_executes_child_against_live_listener_and_emits_run_metadata() {
        let tempdir = tempdir().expect("tempdir");
        let schema_path = tempdir.path().join("schema.sql");
        let report_path = tempdir.path().join("out").join("run.json");
        let snapshot_path = tempdir.path().join("out").join("run.twin");
        let child_path = tempdir.path().join("client.py");
        let port = reserve_local_port();
        write_schema(&schema_path);
        fs::write(
            &child_path,
            r#"import socket
import struct
import sys

host = sys.argv[1]
port = int(sys.argv[2])
sock = socket.create_connection((host, port), timeout=2)

body = struct.pack("!I", 196608)
for name, value in ((b"user", b"postgres"), (b"database", b"postgres")):
    body += name + b"\x00" + value + b"\x00"
body += b"\x00"
sock.sendall(struct.pack("!I", len(body) + 4) + body)

def read_frame():
    header = sock.recv(5)
    if len(header) != 5:
        raise SystemExit("truncated backend frame header")
    tag = header[:1]
    length = struct.unpack("!I", header[1:])[0]
    body = b""
    while len(body) < length - 4:
        chunk = sock.recv(length - 4 - len(body))
        if not chunk:
            raise SystemExit("truncated backend frame body")
        body += chunk
    return tag, body

while True:
    tag, _ = read_frame()
    if tag == b"Z":
        break

query = b"BEGIN\x00"
sock.sendall(b"Q" + struct.pack("!I", len(query) + 4) + query)

while True:
    tag, _ = read_frame()
    if tag == b"Z":
        break

sock.sendall(b"X" + struct.pack("!I", 4))
sock.close()
"#,
        )
        .expect("write child client");

        let config = TwinConfig {
            engine: Engine::Postgres,
            host: "127.0.0.1".to_owned(),
            port,
            schema_path: Some(schema_path),
            verify_path: None,
            run_command: Some(format!(
                "python3 \"{}\" 127.0.0.1 {port}",
                child_path.display()
            )),
            report_path: Some(report_path.clone()),
            snapshot_path: Some(snapshot_path.clone()),
            restore_path: None,
            json: true,
        };

        let execution = execute(config).expect("execution");
        assert_eq!(execution.exit_code, 0);
        let json: serde_json::Value = serde_json::from_str(&execution.stdout).expect("json");
        assert_eq!(json["mode"], "run_once");
        assert_eq!(json["outcome"], "READY");
        assert_eq!(json["port"], port);
        assert_eq!(json["run"]["exit_code"], 0);
        assert_eq!(json["run"]["timed_out"], false);
        assert_eq!(
            json["snapshot"]["written_to"],
            snapshot_path.display().to_string()
        );
        assert!(report_path.exists());
        assert!(snapshot_path.exists());
    }

    #[test]
    fn run_mode_batch_only_verify_refusal_happens_before_run_mode_unimplemented() {
        let tempdir = tempdir().expect("tempdir");
        let schema_path = tempdir.path().join("schema.sql");
        let verify_path = tempdir.path().join("verify.constraint.json");
        let invalid_report_path = tempdir.path().join("report-dir");

        write_schema(&schema_path);
        write_batch_only_live_verify(&verify_path);
        fs::create_dir_all(&invalid_report_path).expect("create invalid report dir");

        let config = TwinConfig {
            engine: Engine::Postgres,
            host: "127.0.0.1".to_owned(),
            port: 5432,
            schema_path: Some(schema_path),
            verify_path: Some(verify_path),
            run_command: Some("python extract.py".to_owned()),
            report_path: Some(invalid_report_path),
            snapshot_path: None,
            restore_path: None,
            json: true,
        };

        let execution = execute(config).expect("execution");
        let json: serde_json::Value = serde_json::from_str(&execution.stdout).expect("json");

        assert_eq!(execution.exit_code, 2);
        assert_eq!(json["refusal"]["code"], "E_BATCH_ONLY_RULE");
        assert_eq!(
            json["refusal"]["detail"]["verify_artifact"]["constraint_set_id"],
            "fixtures.query_rules.orphan_rows"
        );
        assert_eq!(
            json["refusal"]["detail"]["rule_id"],
            "ORPHAN_PROPERTY_TENANT"
        );
    }

    #[test]
    fn run_mode_live_verify_parse_refusal_happens_before_run_mode_unimplemented() {
        let tempdir = tempdir().expect("tempdir");
        let schema_path = tempdir.path().join("schema.sql");
        let verify_path = tempdir.path().join("verify.constraint.json");

        write_schema(&schema_path);
        fs::write(
            &verify_path,
            r#"{
              "version": "verify.constraint.v1",
              "rules": [{ "id": "TENANT_EXISTS" }]
            }"#,
        )
        .expect("write malformed live verify");

        let config = TwinConfig {
            engine: Engine::Postgres,
            host: "127.0.0.1".to_owned(),
            port: 5432,
            schema_path: Some(schema_path),
            verify_path: Some(verify_path),
            run_command: Some("python extract.py".to_owned()),
            report_path: None,
            snapshot_path: None,
            restore_path: None,
            json: true,
        };

        let execution = execute(config).expect("execution");
        let json: serde_json::Value = serde_json::from_str(&execution.stdout).expect("json");

        assert_eq!(execution.exit_code, 2);
        assert_eq!(json["refusal"]["code"], "E_VERIFY_ARTIFACT_PARSE");
    }

    #[test]
    fn missing_schema_refusal_happens_before_output_target_preflight() {
        let tempdir = tempdir().expect("tempdir");
        let report_path = tempdir.path().join("report-dir");
        let snapshot_path = tempdir.path().join("out").join("bootstrap.twin");

        fs::create_dir_all(&report_path).expect("create report dir");

        let config = TwinConfig {
            engine: Engine::Postgres,
            host: "127.0.0.1".to_owned(),
            port: 5432,
            schema_path: Some(tempdir.path().join("missing.sql")),
            verify_path: None,
            run_command: None,
            report_path: Some(report_path),
            snapshot_path: Some(snapshot_path.clone()),
            restore_path: None,
            json: true,
        };

        let execution = execute(config).expect("execution");
        let json: serde_json::Value = serde_json::from_str(&execution.stdout).expect("json");

        assert_eq!(execution.exit_code, 2);
        assert_eq!(json["refusal"]["code"], "E_IO_READ");
        assert!(!snapshot_path.exists(), "snapshot write must not begin");
    }

    #[test]
    fn restore_and_verify_refusals_happen_before_output_target_preflight() {
        let tempdir = tempdir().expect("tempdir");
        let schema_path = tempdir.path().join("schema.sql");
        let malformed_verify_path = tempdir.path().join("verify.json");
        let malformed_snapshot_path = tempdir.path().join("restore.twin");
        let invalid_report_path = tempdir.path().join("report-dir");
        let snapshot_path = tempdir.path().join("out").join("bootstrap.twin");

        fs::write(
            &schema_path,
            "CREATE TABLE public.deals (deal_id TEXT PRIMARY KEY);",
        )
        .expect("write schema");
        fs::write(&malformed_verify_path, "{ not json").expect("write verify");
        fs::write(&malformed_snapshot_path, "{ not json").expect("write snapshot");
        fs::create_dir_all(&invalid_report_path).expect("create report dir");

        let verify_config = TwinConfig {
            engine: Engine::Postgres,
            host: "127.0.0.1".to_owned(),
            port: 5432,
            schema_path: Some(schema_path),
            verify_path: Some(malformed_verify_path),
            run_command: None,
            report_path: Some(invalid_report_path.clone()),
            snapshot_path: Some(snapshot_path.clone()),
            restore_path: None,
            json: true,
        };

        let verify_execution = execute(verify_config).expect("verify execution");
        let verify_json: serde_json::Value =
            serde_json::from_str(&verify_execution.stdout).expect("verify json");
        assert_eq!(verify_execution.exit_code, 2);
        assert_eq!(verify_json["refusal"]["code"], "E_VERIFY_ARTIFACT_PARSE");
        assert!(!snapshot_path.exists(), "snapshot write must not begin");

        let restore_config = TwinConfig {
            engine: Engine::Postgres,
            host: "127.0.0.1".to_owned(),
            port: 5432,
            schema_path: None,
            verify_path: None,
            run_command: None,
            report_path: Some(invalid_report_path),
            snapshot_path: Some(snapshot_path.clone()),
            restore_path: Some(malformed_snapshot_path),
            json: true,
        };

        let restore_execution = execute(restore_config).expect("restore execution");
        let restore_json: serde_json::Value =
            serde_json::from_str(&restore_execution.stdout).expect("restore json");
        assert_eq!(restore_execution.exit_code, 2);
        assert_eq!(restore_json["refusal"]["code"], "E_SNAPSHOT_VERIFY");
        assert!(!snapshot_path.exists(), "snapshot write must not begin");
    }

    #[test]
    fn output_target_preflight_refuses_before_any_artifact_write() {
        let tempdir = tempdir().expect("tempdir");
        let schema_path = tempdir.path().join("schema.sql");

        fs::write(
            &schema_path,
            "CREATE TABLE public.deals (deal_id TEXT PRIMARY KEY);",
        )
        .expect("write schema");

        let invalid_report_path = tempdir.path().join("report-dir");
        fs::create_dir_all(&invalid_report_path).expect("create report dir");
        let blocked_snapshot_path = tempdir.path().join("out").join("bootstrap.twin");

        let report_config = TwinConfig {
            engine: Engine::Postgres,
            host: "127.0.0.1".to_owned(),
            port: 5432,
            schema_path: Some(schema_path.clone()),
            verify_path: None,
            run_command: None,
            report_path: Some(invalid_report_path),
            snapshot_path: Some(blocked_snapshot_path.clone()),
            restore_path: None,
            json: true,
        };

        let report_execution = execute(report_config).expect("report execution");
        let report_json: serde_json::Value =
            serde_json::from_str(&report_execution.stdout).expect("report json");
        assert_eq!(report_execution.exit_code, 2);
        assert_eq!(report_json["refusal"]["code"], "E_IO_WRITE");
        assert!(
            !blocked_snapshot_path.exists(),
            "snapshot write must not begin when report target preflight fails"
        );

        let report_output_path = tempdir.path().join("out").join("bootstrap.json");
        let invalid_snapshot_path = tempdir.path().join("snapshot-dir");
        fs::create_dir_all(&invalid_snapshot_path).expect("create snapshot dir");

        let snapshot_config = TwinConfig {
            engine: Engine::Postgres,
            host: "127.0.0.1".to_owned(),
            port: 5432,
            schema_path: Some(schema_path),
            verify_path: None,
            run_command: None,
            report_path: Some(report_output_path.clone()),
            snapshot_path: Some(invalid_snapshot_path),
            restore_path: None,
            json: true,
        };

        let snapshot_execution = execute(snapshot_config).expect("snapshot execution");
        let snapshot_json: serde_json::Value =
            serde_json::from_str(&snapshot_execution.stdout).expect("snapshot json");
        assert_eq!(snapshot_execution.exit_code, 2);
        assert_eq!(snapshot_json["refusal"]["code"], "E_IO_WRITE");
        assert!(
            !report_output_path.exists(),
            "report write must not begin when snapshot target preflight fails"
        );
    }

    #[test]
    fn exit_domain_keeps_child_failures_in_run_metadata_not_process_exit() {
        let success = RunReport {
            command: String::from("python extract.py"),
            exit_code: Some(0),
            signal: None,
            timed_out: false,
        };
        let child_failure = RunReport {
            command: String::from("python extract.py"),
            exit_code: Some(7),
            signal: None,
            timed_out: false,
        };
        let timeout = RunReport {
            command: String::from("python extract.py"),
            exit_code: None,
            signal: Some(9),
            timed_out: true,
        };

        assert_eq!(execution_exit_domain(None, false), 0);
        assert_eq!(execution_exit_domain(Some(&success), false), 0);
        assert_eq!(execution_exit_domain(Some(&success), true), 1);
        assert_eq!(execution_exit_domain(Some(&child_failure), false), 0);
        assert_eq!(execution_exit_domain(Some(&timeout), false), 0);
        assert_eq!(execution_exit_domain(Some(&child_failure), true), 1);
        assert_eq!(execution_exit_domain(Some(&timeout), true), 1);
    }

    #[test]
    fn run_mode_verify_failures_raise_process_exit_one() {
        let tempdir = tempdir().expect("tempdir");
        let schema_path = tempdir.path().join("schema.sql");
        let snapshot_path = tempdir.path().join("restore.twin");
        let verify_path = tempdir.path().join("verify.constraint.json");
        let port = reserve_local_port();

        fs::write(
            &schema_path,
            "CREATE TABLE public.deals (deal_id TEXT PRIMARY KEY, deal_name TEXT);",
        )
        .expect("write schema");
        fs::write(
            &verify_path,
            r#"{
              "version": "verify.constraint.v1",
              "constraint_set_id": "fixtures.public.deals.not_null",
              "bindings": [
                { "name": "public.deals", "kind": "relation", "key_fields": ["deal_id"] }
              ],
              "rules": [
                {
                  "id": "DEAL_NAME_PRESENT",
                  "severity": "error",
                  "portability": "portable",
                  "check": { "op": "not_null", "binding": "public.deals", "columns": ["deal_name"] }
                }
              ]
            }"#,
        )
        .expect("write verify");

        let catalog = catalog::parse_postgres_schema(
            "CREATE TABLE public.deals (deal_id TEXT PRIMARY KEY, deal_name TEXT);",
        )
        .expect("parse schema");
        let snapshot = TwinSnapshot::new(
            Engine::Postgres,
            schema_path.display().to_string(),
            String::from("sha256:schema"),
            None,
            None,
            catalog,
        )
        .expect("snapshot")
        .with_relations(BTreeMap::from([(
            String::from("public.deals"),
            vec![BTreeMap::from([
                (
                    String::from("deal_id"),
                    json!({ "kind": "text", "value": "deal-1" }),
                ),
                (String::from("deal_name"), json!({ "kind": "null" })),
            ])],
        )]))
        .expect("snapshot with relations");
        snapshot::write_snapshot(&snapshot_path, &snapshot).expect("write snapshot");

        let config = TwinConfig {
            engine: Engine::Postgres,
            host: "127.0.0.1".to_owned(),
            port,
            schema_path: None,
            verify_path: Some(verify_path),
            run_command: Some(String::from("true")),
            report_path: None,
            snapshot_path: None,
            restore_path: Some(snapshot_path),
            json: true,
        };

        let execution = execute(config).expect("execution");
        assert_eq!(execution.exit_code, 1);

        let json: serde_json::Value = serde_json::from_str(&execution.stdout).expect("json");
        assert_eq!(json["mode"], "run_once");
        assert_eq!(json["outcome"], "FAIL");
        assert_eq!(json["run"]["exit_code"], 0);
        assert_eq!(json["verify"]["outcome"], "FAIL");
    }

    fn reserve_local_port() -> u16 {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind local port");
        let port = listener.local_addr().expect("listener addr").port();
        drop(listener);
        port
    }
}
