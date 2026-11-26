use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnowflakeConfig {
    pub schema_path: Option<PathBuf>,
    pub host: String,
    pub port: u16,
    pub run_command: Option<String>,
    /// `--serve`: run the interactive HTTP twin until SIGINT/SIGTERM. With neither `serve` nor
    /// `run_command`, the twin only validates, writes the report and exits (no listener).
    pub serve: bool,
    pub report_path: Option<PathBuf>,
    pub materialize_source_url: Option<String>,
    pub max_rows_per_table: usize,
    pub json: bool,
}

impl SnowflakeConfig {
    pub fn from_parts(parts: SnowflakeConfigParts, json: bool) -> Self {
        Self {
            schema_path: parts.schema_path,
            host: parts.host,
            port: parts.port,
            run_command: parts.run_command,
            serve: parts.serve,
            report_path: parts.report_path,
            materialize_source_url: parts.materialize_source_url,
            max_rows_per_table: parts.max_rows_per_table,
            json,
        }
    }
}

impl SnowflakeConfig {
    /// Neither `--serve` nor `--run`: validate the schema, write the report, print the readiness
    /// summary and exit without binding a port (same flag contract as `twinning postgres`).
    pub fn is_bootstrap_only(&self) -> bool {
        !self.serve && self.run_command.is_none()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnowflakeConfigParts {
    pub schema_path: Option<PathBuf>,
    pub host: String,
    pub port: u16,
    pub run_command: Option<String>,
    pub serve: bool,
    pub report_path: Option<PathBuf>,
    pub materialize_source_url: Option<String>,
    pub max_rows_per_table: usize,
}
