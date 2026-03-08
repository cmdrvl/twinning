use std::path::Path;

use sha2::{Digest, Sha256};

use crate::{
    catalog::{self, Catalog},
    cli::Engine,
    config::TwinConfig,
    refusal,
    refusal::RefusalResult,
    report::{SchemaReport, SnapshotReport, TwinReport, TwinReportSeed, VerifyArtifactReport},
    snapshot::{self, TwinSnapshot},
};

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
    catalog: Catalog,
    restored_from: Option<String>,
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

    if config.run_command.is_some() {
        return Err(Box::new(refusal::run_mode_unimplemented(config)));
    }

    let state = if let Some(restore_path) = &config.restore_path {
        restore_state(config, restore_path)?
    } else {
        load_state_from_schema(config)?
    };

    let mut snapshot_report = SnapshotReport {
        restored_from: state.restored_from.clone(),
        written_to: None,
        snapshot_hash: None,
    };

    if let Some(snapshot_path) = &config.snapshot_path {
        let snapshot = TwinSnapshot::new(
            config.engine,
            state.schema_source.clone(),
            state.schema_hash.clone(),
            state.verify_artifact.clone(),
            state.catalog.clone(),
        )?;
        let snapshot_hash = snapshot::write_snapshot(snapshot_path, &snapshot)?;
        snapshot_report.written_to = Some(path_display(snapshot_path));
        snapshot_report.snapshot_hash = Some(snapshot_hash);
    }

    let report = TwinReport::from_seed(TwinReportSeed {
        engine: config.engine.as_str(),
        host: &config.host,
        port: config.port,
        schema: SchemaReport {
            source: state.schema_source.clone(),
            hash: state.schema_hash.clone(),
            table_count: state.catalog.table_count,
            column_count: state.catalog.column_count,
            index_count: state.catalog.index_count,
            constraint_count: state.catalog.constraint_count,
        },
        verify_artifact: state.verify_artifact.clone(),
        catalog: &state.catalog,
        snapshot: snapshot_report,
        warnings: state.warnings,
    });

    if let Some(report_path) = &config.report_path {
        write_json(report_path, &report)?;
    }

    let stdout = if config.json {
        report
            .render_json()
            .map_err(|error| Box::new(refusal::serialization(error.to_string())))?
    } else {
        report.render_human()
    };

    Ok(Execution {
        exit_code: 0,
        stdout,
    })
}

fn load_state_from_schema(config: &TwinConfig) -> RefusalResult<BootstrapState> {
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
        verify_artifact: load_verify_artifact(config.verify_path.as_deref())?,
        catalog,
        restored_from: None,
        warnings: Vec::new(),
    })
}

fn restore_state(config: &TwinConfig, restore_path: &Path) -> RefusalResult<BootstrapState> {
    let snapshot = snapshot::read_snapshot(restore_path)?;
    let verify_artifact = match config.verify_path.as_deref() {
        Some(path) => load_verify_artifact(Some(path))?,
        None => snapshot.verify_artifact.clone(),
    };

    Ok(BootstrapState {
        schema_source: snapshot.schema_source,
        schema_hash: snapshot.schema_hash,
        verify_artifact,
        catalog: snapshot.catalog,
        restored_from: Some(path_display(restore_path)),
        warnings: Vec::new(),
    })
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

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf};

    use tempfile::tempdir;

    use crate::{cli::Engine, config::TwinConfig};

    use super::execute;

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
        assert_eq!(json["verify"]["loaded"], 1);
    }

    #[test]
    fn run_mode_is_refused_for_now() {
        let config = TwinConfig {
            engine: Engine::Postgres,
            host: "127.0.0.1".to_owned(),
            port: 5432,
            schema_path: Some(PathBuf::from("schema.sql")),
            verify_path: None,
            run_command: Some("python extract.py".to_owned()),
            report_path: None,
            snapshot_path: None,
            restore_path: None,
            json: true,
        };

        let execution = execute(config).expect("execution");
        assert_eq!(execution.exit_code, 2);
        let json: serde_json::Value = serde_json::from_str(&execution.stdout).expect("json");
        assert_eq!(json["refusal"]["code"], "E_RUN_MODE_UNIMPLEMENTED");
    }
}
