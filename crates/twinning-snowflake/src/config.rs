use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnowflakeConfig {
    pub schema_path: Option<PathBuf>,
    pub host: String,
    pub port: u16,
    pub run_command: Option<String>,
    pub serve: bool,
    pub serve_defaulted: bool,
    pub report_path: Option<PathBuf>,
    pub materialize_source_url: Option<String>,
    pub max_rows_per_table: usize,
    pub json: bool,
}

impl SnowflakeConfig {
    pub fn from_parts(parts: SnowflakeConfigParts, json: bool) -> Self {
        let serve_defaulted = !parts.serve && parts.run_command.is_none();
        Self {
            schema_path: parts.schema_path,
            host: parts.host,
            port: parts.port,
            run_command: parts.run_command,
            serve: parts.serve || serve_defaulted,
            serve_defaulted,
            report_path: parts.report_path,
            materialize_source_url: parts.materialize_source_url,
            max_rows_per_table: parts.max_rows_per_table,
            json,
        }
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
