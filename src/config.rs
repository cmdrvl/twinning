use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{
    cli::{Cli, Engine, TwinArgs},
    refusal,
    refusal::RefusalResult,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TwinConfig {
    pub engine: Engine,
    pub host: String,
    pub port: u16,
    pub schema_path: Option<PathBuf>,
    pub verify_path: Option<PathBuf>,
    pub declaration_path: Option<PathBuf>,
    pub run_command: Option<String>,
    pub serve: bool,
    pub report_path: Option<PathBuf>,
    pub snapshot_path: Option<PathBuf>,
    pub query_trace_path: Option<PathBuf>,
    pub restore_path: Option<PathBuf>,
    pub materialize_source_url: Option<String>,
    pub json: bool,
}

impl TwinConfig {
    pub fn from_cli(cli: &Cli) -> RefusalResult<Self> {
        let Some(command) = &cli.command else {
            return Err(Box::new(refusal::missing_command()));
        };
        let engine = command
            .engine()
            .ok_or_else(|| Box::new(refusal::missing_command()))?;
        let args = command
            .twin_args()
            .ok_or_else(|| Box::new(refusal::missing_command()))?;

        Self::from_engine_args(engine, args, cli.json)
    }

    pub fn from_engine_args(engine: Engine, args: &TwinArgs, json: bool) -> RefusalResult<Self> {
        if args.schema.is_none() && args.restore.is_none() {
            return Err(Box::new(refusal::missing_bootstrap_source(engine)));
        }

        if args.schema.is_some() && args.restore.is_some() {
            return Err(Box::new(refusal::ambiguous_bootstrap_source()));
        }

        if args.restore.is_some() && args.materialize_source_url.is_some() {
            return Err(Box::new(refusal::materialization_requires_schema()));
        }

        if args.serve && args.run.is_some() {
            return Err(Box::new(refusal::ambiguous_live_mode(engine)));
        }

        Ok(Self {
            engine,
            host: args.host.clone(),
            port: args.port.unwrap_or_else(|| engine.default_port()),
            schema_path: args.schema.clone(),
            verify_path: args.verify.clone(),
            declaration_path: args.declaration.clone(),
            run_command: args.run.clone(),
            serve: args.serve,
            report_path: args.report.clone(),
            snapshot_path: args.snapshot.clone(),
            query_trace_path: args.query_trace.clone(),
            restore_path: args.restore.clone(),
            materialize_source_url: args.materialize_source_url.clone(),
            json,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::cli::{Engine, TwinArgs};

    use super::TwinConfig;

    #[test]
    fn config_uses_engine_default_port() {
        let args = TwinArgs {
            schema: Some("schema.sql".into()),
            verify: None,
            declaration: None,
            host: "127.0.0.1".to_owned(),
            port: None,
            run: None,
            serve: false,
            report: None,
            snapshot: None,
            query_trace: None,
            restore: None,
            materialize_source_url: None,
        };

        let config =
            TwinConfig::from_engine_args(Engine::Mysql, &args, false).expect("config should build");
        assert_eq!(config.port, 3306);
    }
}
