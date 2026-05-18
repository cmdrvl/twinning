//! Newline-delimited JSON-RPC stdio transport for MCP clients.

use std::{
    fs,
    io::{self, BufRead, Write},
    path::Path,
};

use serde_json::{Value as JsonValue, json};

use crate::{
    refusal::{self, RefusalResult},
    runtime::Execution,
};

use super::{
    catalog::{McpCatalog, load_mcp_catalog_from_manifest, load_mcp_catalog_from_server},
    dispatcher::{
        JSONRPC_PARSE_ERROR, JsonRpcError, JsonRpcRequest, JsonRpcResponse, ToolExecutability,
        classify_tool_executability, dispatch,
    },
    listener::{McpCatalogInput, McpConfig},
    report::{McpProtocolVersionLog, McpReport, McpRequestLog, McpSessionLog, McpToolCallOutcome},
    version::negotiate_initialize_protocol,
};

pub fn run_stdio(config: McpConfig) -> Result<Execution, Box<dyn std::error::Error>> {
    let json_mode = config.json;
    let rendered = match run_stdio_inner(config) {
        Ok(execution) => execution,
        Err(refusal) => Execution {
            exit_code: 2,
            stdout: refusal.render(json_mode)?,
        },
    };

    Ok(rendered)
}

fn run_stdio_inner(config: McpConfig) -> RefusalResult<Execution> {
    let catalog = load_catalog(&config.source)?;
    let mut session_log = McpSessionLog::default();
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();

    for line in stdin.lock().lines() {
        let line = line
            .map_err(|error| Box::new(refusal::runtime_io("mcp_stdio_read", error.to_string())))?;
        if line.trim().is_empty() {
            continue;
        }
        let outcome = handle_jsonrpc_line(&line, &catalog);
        session_log.record(outcome.log);
        if let Some(response) = outcome.response {
            let rendered = serde_json::to_string(&response).map_err(|error| {
                Box::new(refusal::runtime_io("mcp_stdio_render", error.to_string()))
            })?;
            writeln!(stdout, "{rendered}").map_err(|error| {
                Box::new(refusal::runtime_io("mcp_stdio_write", error.to_string()))
            })?;
            stdout.flush().map_err(|error| {
                Box::new(refusal::runtime_io("mcp_stdio_flush", error.to_string()))
            })?;
        }
    }

    write_mcp_report_if_requested(&config, &catalog, session_log)?;

    Ok(Execution {
        exit_code: 0,
        stdout: String::new(),
    })
}

struct StdioDispatchOutcome {
    response: Option<JsonRpcResponse>,
    log: McpRequestLog,
}

fn handle_jsonrpc_line(line: &str, catalog: &McpCatalog) -> StdioDispatchOutcome {
    let request = match serde_json::from_str::<JsonRpcRequest>(line) {
        Ok(request) => request,
        Err(error) => {
            return StdioDispatchOutcome {
                response: Some(error_response(
                    JSONRPC_PARSE_ERROR,
                    "parse error",
                    Some(json!({ "detail": error.to_string() })),
                )),
                log: McpRequestLog {
                    method: None,
                    refusal: true,
                    tool_call: None,
                    protocol_version: None,
                },
            };
        }
    };

    let method = request.method.clone();
    let tool_call = request_tool_call_outcome(&request, catalog);
    let protocol_version = request_protocol_version_log(&request);
    let response = dispatch(request, catalog);
    let refusal = response
        .as_ref()
        .is_some_and(|response| response.error.is_some());

    StdioDispatchOutcome {
        response,
        log: McpRequestLog {
            method: Some(method),
            refusal,
            tool_call,
            protocol_version,
        },
    }
}

fn load_catalog(source: &McpCatalogInput) -> RefusalResult<McpCatalog> {
    match source {
        McpCatalogInput::LiveServer { command } => load_mcp_catalog_from_server(command),
        McpCatalogInput::Manifest { path } => load_mcp_catalog_from_manifest(path),
    }
}

fn request_tool_call_outcome(
    request: &JsonRpcRequest,
    catalog: &McpCatalog,
) -> Option<McpToolCallOutcome> {
    if request.method != "tools/call" {
        return None;
    }
    let name = request
        .params
        .as_ref()
        .and_then(|params| params.get("name"))
        .and_then(JsonValue::as_str)?;
    let tool = catalog.tools.iter().find(|tool| tool.name == name)?;
    match classify_tool_executability(tool) {
        ToolExecutability::Stubbable => Some(McpToolCallOutcome::Stubbable),
        ToolExecutability::UnsupportedShape => Some(McpToolCallOutcome::Unsupported),
    }
}

fn request_protocol_version_log(request: &JsonRpcRequest) -> Option<McpProtocolVersionLog> {
    if request.method != "initialize" {
        return None;
    }
    let negotiated = negotiate_initialize_protocol(request.params.as_ref());
    Some(McpProtocolVersionLog {
        requested: negotiated.requested,
        negotiated: negotiated.negotiated,
        supported: negotiated.supported,
    })
}

fn error_response(
    code: i64,
    message: impl Into<String>,
    data: Option<JsonValue>,
) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".to_owned(),
        id: JsonValue::Null,
        result: None,
        error: Some(JsonRpcError {
            code,
            message: message.into(),
            data,
        }),
    }
}

fn write_mcp_report_if_requested(
    config: &McpConfig,
    catalog: &McpCatalog,
    session_log: McpSessionLog,
) -> RefusalResult<()> {
    let Some(path) = &config.report_path else {
        return Ok(());
    };
    let report = McpReport::new(catalog, session_log.summary());
    write_json_report(path, &report)
}

fn write_json_report(path: &Path, report: &McpReport) -> RefusalResult<()> {
    let rendered = report
        .render_json()
        .map_err(|error| Box::new(refusal::runtime_io("mcp_report_render", error.to_string())))?;
    fs::write(path, rendered).map_err(|error| Box::new(refusal::io_write(path, &error)))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::mcp::{
        catalog::{McpCatalog, McpCatalogSource, McpServerInfo, McpTool},
        stdio::handle_jsonrpc_line,
    };

    #[test]
    fn stdio_line_dispatches_jsonrpc_and_records_protocol_warning() {
        let catalog = catalog();
        let outcome = handle_jsonrpc_line(
            &json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": { "protocolVersion": "2099-01-01" }
            })
            .to_string(),
            &catalog,
        );

        let response = outcome.response.expect("initialize response");
        assert_eq!(
            "2025-03-26",
            response.result.expect("result")["protocolVersion"]
        );
        assert_eq!(Some("initialize".to_owned()), outcome.log.method);
        assert_eq!(
            Some("2099-01-01".to_owned()),
            outcome
                .log
                .protocol_version
                .as_ref()
                .and_then(|version| version.requested.clone())
        );
        assert!(
            !outcome
                .log
                .protocol_version
                .expect("protocol log")
                .supported
        );
    }

    fn catalog() -> McpCatalog {
        McpCatalog {
            server_info: McpServerInfo {
                name: "demo".to_owned(),
                version: "1.0.0".to_owned(),
            },
            tools: vec![McpTool {
                name: "lookup".to_owned(),
                description: None,
                input_schema: json!({ "type": "object" }),
                output_schema: Some(json!({ "type": "object" })),
            }],
            resources: Vec::new(),
            prompts: Vec::new(),
            security_schemes: Vec::new(),
            required_auth_schemes: Vec::new(),
            catalog_hash: "sha256:test".to_owned(),
            source: McpCatalogSource::Manifest {
                path: "mcp.json".to_owned(),
            },
        }
    }

    #[test]
    fn parse_errors_return_jsonrpc_parse_error() {
        let outcome = handle_jsonrpc_line("not json", &catalog());
        let error = outcome
            .response
            .expect("parse error response")
            .error
            .expect("error");

        assert_eq!(super::JSONRPC_PARSE_ERROR, error.code);
        assert!(outcome.log.refusal);
    }

    #[test]
    fn notifications_emit_no_stdio_response() {
        let outcome = handle_jsonrpc_line(
            &json!({
                "jsonrpc": "2.0",
                "method": "notifications/initialized"
            })
            .to_string(),
            &catalog(),
        );

        assert!(outcome.response.is_none());
        assert_eq!(
            Some("notifications/initialized".to_owned()),
            outcome.log.method
        );
    }
}
