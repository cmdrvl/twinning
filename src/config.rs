use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{
    cli::{Cli, Engine},
    refusal,
    refusal::RefusalResult,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TwinConfig {
    pub engine: Engine,
    pub host: String,
    pub port: u16,
    pub schema_path: Option<PathBuf>,
    pub rules_path: Option<PathBuf>,
    pub run_command: Option<String>,
    pub report_path: Option<PathBuf>,
    pub snapshot_path: Option<PathBuf>,
    pub restore_path: Option<PathBuf>,
    pub json: bool,
}

impl TwinConfig {
    pub fn from_cli(cli: &Cli) -> RefusalResult<Self> {
        if cli.schema.is_none() && cli.restore.is_none() {
            return Err(Box::new(refusal::missing_bootstrap_source(cli.engine)));
        }

        if cli.schema.is_some() && cli.restore.is_some() {
            return Err(Box::new(refusal::ambiguous_bootstrap_source()));
        }

        Ok(Self {
            engine: cli.engine,
            host: cli.host.clone(),
            port: cli.port.unwrap_or_else(|| cli.engine.default_port()),
            schema_path: cli.schema.clone(),
            rules_path: cli.rules.clone(),
            run_command: cli.run.clone(),
            report_path: cli.report.clone(),
            snapshot_path: cli.snapshot.clone(),
            restore_path: cli.restore.clone(),
            json: cli.json,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::cli::{Cli, Engine};

    use super::TwinConfig;

    #[test]
    fn config_uses_engine_default_port() {
        let cli = Cli {
            engine: Engine::Mysql,
            schema: Some("schema.sql".into()),
            rules: None,
            host: "127.0.0.1".to_owned(),
            port: None,
            run: None,
            report: None,
            snapshot: None,
            restore: None,
            json: false,
            describe: false,
        };

        let config = TwinConfig::from_cli(&cli).expect("config should build");
        assert_eq!(config.port, 3306);
    }
}
