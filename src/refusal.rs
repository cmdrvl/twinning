pub use twinning_kernel::refusal::*;

#[cfg(any(feature = "rest", feature = "mcp", feature = "snowflake"))]
use serde_json::json;

#[cfg(feature = "rest")]
use crate::config::RestConfig;

#[cfg(feature = "rest")]
pub fn missing_rest_spec() -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_REST_SPEC_REQUIRED",
        "Provide --spec <FILE> for the OpenAPI document.",
        json!({ "protocol": "rest" }),
        Some("twinning rest --spec api.yaml --json".to_owned()),
    )
}

#[cfg(feature = "rest")]
pub fn ambiguous_rest_live_mode() -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_AMBIGUOUS_LIVE_MODE",
        "Use either --run for one child command or --serve for standalone interactive mode, not both.",
        json!({ "protocol": "rest", "disallowed_combination": ["run", "serve"] }),
        Some("twinning rest --spec api.yaml --serve --json".to_owned()),
    )
}

#[cfg(feature = "rest")]
pub fn invalid_rest_server_variable(raw: &str, reason: &str) -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_REST_INVALID_SERVER_VARIABLE",
        "Invalid REST OpenAPI server variable selection.",
        json!({ "protocol": "rest", "server_variable": raw, "reason": reason }),
        Some("twinning rest --spec api.yaml --server-variable basePath=v3 --json".to_owned()),
    )
}

#[cfg(feature = "mcp")]
pub fn missing_mcp_catalog_source() -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_MCP_SOURCE_REQUIRED",
        "Provide exactly one MCP catalog source: --server <COMMAND> or --manifest <FILE>.",
        json!({ "protocol": "mcp" }),
        Some("twinning mcp --manifest mcp.json --json".to_owned()),
    )
}

#[cfg(feature = "mcp")]
pub fn ambiguous_mcp_catalog_source() -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_MCP_SOURCE_AMBIGUOUS",
        "Use either --server for live MCP introspection or --manifest for a static catalog, not both.",
        json!({ "protocol": "mcp", "disallowed_combination": ["server", "manifest"] }),
        Some("twinning mcp --manifest mcp.json --json".to_owned()),
    )
}

#[cfg(feature = "mcp")]
pub fn mcp_stdio_run_unsupported() -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_MCP_STDIO_RUN_UNSUPPORTED",
        "Use --stdio for line-delimited stdin/stdout mode or --run for HTTP run-once mode, not both.",
        json!({ "protocol": "mcp", "disallowed_combination": ["stdio", "run"] }),
        Some("twinning mcp --stdio --manifest mcp.json --json".to_owned()),
    )
}

#[cfg(feature = "snowflake")]
pub fn missing_snowflake_schema() -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_SNOWFLAKE_SCHEMA_REQUIRED",
        "Provide --schema <FILE> for the Snowflake DDL catalog seed.",
        json!({ "protocol": "snowflake" }),
        Some("twinning snowflake --schema schema.sql --json".to_owned()),
    )
}

#[cfg(feature = "snowflake")]
pub fn ambiguous_snowflake_live_mode() -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_AMBIGUOUS_LIVE_MODE",
        "Use either --run for one child command or --serve for standalone interactive mode, not both.",
        json!({ "protocol": "snowflake", "disallowed_combination": ["run", "serve"] }),
        Some("twinning snowflake --schema schema.sql --serve --json".to_owned()),
    )
}

#[cfg(feature = "rest")]
pub fn rest_listener_unimplemented(config: &RestConfig) -> RefusalEnvelope {
    let startup_notice = if config.serve_defaulted {
        Some(format!(
            "No --run or --serve specified \u{2014} defaulting to interactive serve mode on http://{}:{}",
            config.host, config.port
        ))
    } else {
        None
    };
    let bind_warning = if config.host == "0.0.0.0" {
        Some(
            "WARNING: REST twin binding to 0.0.0.0 \u{2014} accessible on all interfaces. Use only in trusted network environments.",
        )
    } else {
        None
    };
    let strict_warning = if config.strict {
        Some("--strict response validation is not yet implemented")
    } else {
        None
    };

    RefusalEnvelope::new(
        "E_REST_LISTENER_UNIMPLEMENTED",
        "REST CLI/config dispatch is wired, but the HTTP listener is not implemented in this build.",
        json!({
            "protocol": "rest",
            "spec": config.spec_path.display().to_string(),
            "host": config.host,
            "port": config.port,
            "mode": if config.run_command.is_some() { "run_once" } else { "serve" },
            "serve": config.serve,
            "serve_defaulted": config.serve_defaulted,
            "run": config.run_command,
            "report": config.report_path.as_ref().map(|path| path.display().to_string()),
            "canary": config.canary_path.as_ref().map(|path| path.display().to_string()),
            "strict": config.strict,
            "startup_notice": startup_notice,
            "bind_warning": bind_warning,
            "strict_warning": strict_warning,
        }),
        Some("twinning doctor capabilities --json".to_owned()),
    )
}
