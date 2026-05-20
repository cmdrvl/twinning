use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::Engine;

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
    pub export_seed_contract_path: Option<PathBuf>,
    pub seed_path: Option<PathBuf>,
    pub materialize_source_url: Option<String>,
    pub json: bool,
}
