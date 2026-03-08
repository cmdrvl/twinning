use std::path::Path;

use serde::Serialize;
use serde_json::{Value, json};

use crate::{cli::Engine, config::TwinConfig};

pub const VERSION: &str = "twinning.v0";
pub type RefusalResult<T> = Result<T, Box<RefusalEnvelope>>;

#[derive(Debug, Clone, Serialize)]
pub struct RefusalEnvelope {
    version: String,
    outcome: &'static str,
    refusal: Refusal,
}

#[derive(Debug, Clone, Serialize)]
pub struct Refusal {
    code: String,
    message: String,
    detail: Value,
    next_command: Option<String>,
}

impl RefusalEnvelope {
    pub fn new(
        code: impl Into<String>,
        message: impl Into<String>,
        detail: Value,
        next_command: Option<String>,
    ) -> Self {
        Self {
            version: VERSION.to_owned(),
            outcome: "REFUSAL",
            refusal: Refusal {
                code: code.into(),
                message: message.into(),
                detail,
                next_command,
            },
        }
    }

    pub fn render(&self, json_mode: bool) -> Result<String, serde_json::Error> {
        if json_mode {
            let mut rendered = serde_json::to_string_pretty(self)?;
            rendered.push('\n');
            return Ok(rendered);
        }

        let mut rendered = format!(
            "REFUSAL [{}]\n{}\n",
            self.refusal.code, self.refusal.message
        );
        if !self.refusal.detail.is_null() && self.refusal.detail != json!({}) {
            rendered.push_str(&format!("detail: {}\n", self.refusal.detail));
        }
        if let Some(next_command) = &self.refusal.next_command {
            rendered.push_str(&format!("next: {next_command}\n"));
        }
        Ok(rendered)
    }
}

pub fn missing_bootstrap_source(engine: Engine) -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_BOOTSTRAP_SOURCE_REQUIRED",
        "Provide either --schema <FILE> or --restore <FILE>.",
        json!({ "engine": engine.as_str() }),
        Some(format!(
            "twinning {} --schema schema.sql --json",
            engine.as_str()
        )),
    )
}

pub fn ambiguous_bootstrap_source() -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_AMBIGUOUS_BOOTSTRAP_SOURCE",
        "Use exactly one bootstrap source: --schema or --restore, not both.",
        json!({}),
        Some("twinning postgres --schema schema.sql --json".to_owned()),
    )
}

pub fn engine_unimplemented(engine: Engine) -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_ENGINE_UNIMPLEMENTED",
        format!(
            "`{}` is declared in the CLI, but this build only implements the Postgres bootstrap path.",
            engine.as_str()
        ),
        json!({
            "requested_engine": engine.as_str(),
            "implemented_engine": "postgres"
        }),
        Some("twinning postgres --schema schema.sql --json".to_owned()),
    )
}

pub fn run_mode_unimplemented(config: &TwinConfig) -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_RUN_MODE_UNIMPLEMENTED",
        "Live command execution depends on the wire-protocol server, which is not implemented in this build.",
        json!({
            "engine": config.engine.as_str(),
            "host": config.host,
            "port": config.port,
            "run": config.run_command
        }),
        Some(format!(
            "twinning {} --schema schema.sql --report bootstrap.json --json",
            config.engine.as_str()
        )),
    )
}

pub fn io_read(path: &Path, error: &std::io::Error) -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_IO_READ",
        format!("Failed to read `{}`.", path.display()),
        json!({ "path": path.display().to_string(), "error": error.to_string() }),
        None,
    )
}

pub fn io_write(path: &Path, error: &std::io::Error) -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_IO_WRITE",
        format!("Failed to write `{}`.", path.display()),
        json!({ "path": path.display().to_string(), "error": error.to_string() }),
        None,
    )
}

pub fn schema_parse(path: &Path, message: impl Into<String>) -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_SCHEMA_PARSE",
        format!("Schema bootstrap failed for `{}`.", path.display()),
        json!({ "path": path.display().to_string(), "error": message.into() }),
        Some(format!(
            "twinning postgres --schema {} --json",
            path.display()
        )),
    )
}

pub fn verify_artifact_parse(path: &Path, message: impl Into<String>) -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_VERIFY_ARTIFACT_PARSE",
        format!(
            "Compiled verify artifact bootstrap failed for `{}`.",
            path.display()
        ),
        json!({ "path": path.display().to_string(), "error": message.into() }),
        None,
    )
}

pub fn snapshot_verify(path: &Path, message: impl Into<String>) -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_SNAPSHOT_VERIFY",
        format!("Snapshot verification failed for `{}`.", path.display()),
        json!({ "path": path.display().to_string(), "error": message.into() }),
        Some(format!(
            "twinning postgres --schema schema.sql --snapshot {} --json",
            path.display()
        )),
    )
}

pub fn serialization(message: impl Into<String>) -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_SERIALIZATION",
        "Failed to serialize twinning output.",
        json!({ "error": message.into() }),
        None,
    )
}
