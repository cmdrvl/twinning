//! MCP JSON-RPC method routing and stdio serving.

use std::{
    io::{self, stdin, stdout},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use serde_json::{Value, json};
use signal_hook::{
    consts::signal::{SIGINT, SIGTERM},
    flag,
};
use twinning_mcp_core::{
    JsonRpcRequest, JsonRpcResponse,
    jsonrpc::{
        JSONRPC_INVALID_PARAMS, JSONRPC_INVALID_REQUEST, JSONRPC_METHOD_NOT_FOUND, error_response,
        result_response,
    },
    stdio,
    version::negotiate_initialize_protocol,
};

use crate::{errors::ToolError, tools::tool_list_result};

pub fn serve() -> io::Result<()> {
    let stopping = Arc::new(AtomicBool::new(false));
    flag::register(SIGINT, Arc::clone(&stopping))?;
    flag::register(SIGTERM, Arc::clone(&stopping))?;

    stdio::serve_until(stdin().lock(), stdout().lock(), dispatch, || {
        stopping.load(Ordering::Relaxed)
    })
}

pub fn dispatch(request: JsonRpcRequest) -> Option<JsonRpcResponse> {
    if request.id.is_none() && request.method.starts_with("notifications/") {
        return None;
    }

    let id = request.id.clone().unwrap_or(Value::Null);
    if request.jsonrpc != "2.0" {
        return Some(error_response(
            id,
            JSONRPC_INVALID_REQUEST,
            "invalid JSON-RPC version",
            None,
        ));
    }

    Some(match request.method.as_str() {
        "initialize" => result_response(id, initialize_result(request.params.as_ref())),
        "notifications/initialized" => return None,
        "tools/list" => result_response(id, tool_list_result()),
        "tools/call" => tools_call_response(id, request.params.as_ref()),
        "ping" => result_response(id, json!({})),
        method => error_response(
            id,
            JSONRPC_METHOD_NOT_FOUND,
            format!("method not found: {method}"),
            None,
        ),
    })
}

fn initialize_result(params: Option<&Value>) -> Value {
    let protocol = negotiate_initialize_protocol(params);
    json!({
        "protocolVersion": protocol.negotiated,
        "capabilities": {
            "tools": { "listChanged": false }
        },
        "serverInfo": {
            "name": "twinning-mcp",
            "version": env!("CARGO_PKG_VERSION")
        }
    })
}

fn tools_call_response(id: Value, params: Option<&Value>) -> JsonRpcResponse {
    let Some(name) = params
        .and_then(|params| params.get("name"))
        .and_then(Value::as_str)
    else {
        return error_response(
            id,
            JSONRPC_INVALID_PARAMS,
            "tools/call params must include string field `name`",
            None,
        );
    };

    let result = ToolError::not_implemented(name).mcp_result();
    result_response(id, result)
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};
    use twinning_mcp_core::{JsonRpcRequest, jsonrpc::JSONRPC_METHOD_NOT_FOUND};

    use super::dispatch;

    fn request(id: Option<Value>, method: &str, params: Option<Value>) -> JsonRpcRequest {
        JsonRpcRequest {
            jsonrpc: "2.0".to_owned(),
            method: method.to_owned(),
            params,
            id,
        }
    }

    #[test]
    fn u01_unknown_method_is_method_not_found_and_ping_succeeds() {
        let unknown = dispatch(request(Some(json!(1)), "not/a/method", None))
            .expect("requests receive a response");
        assert_eq!(
            Some(JSONRPC_METHOD_NOT_FOUND),
            unknown.error.map(|error| error.code)
        );

        let ping = dispatch(request(Some(json!(2)), "ping", None)).expect("ping response");
        assert_eq!(Some(json!({})), ping.result);
        assert_eq!(None, ping.error);
    }

    #[test]
    fn invalid_jsonrpc_version_returns_invalid_request_with_original_id() {
        let mut invalid = request(Some(json!(5)), "ping", None);
        invalid.jsonrpc = "1.0".to_owned();
        let response = dispatch(invalid).expect("invalid request response");
        assert_eq!(json!(5), response.id);
        assert_eq!(
            Some(twinning_mcp_core::JSONRPC_INVALID_REQUEST),
            response.error.map(|error| error.code)
        );
    }

    #[test]
    fn notifications_and_initialized_handshake_do_not_emit_responses() {
        assert!(dispatch(request(None, "notifications/initialized", None)).is_none());
        assert!(dispatch(request(None, "notifications/other", None)).is_none());
    }

    #[test]
    fn tools_call_is_an_error_result_until_m4() {
        let response = dispatch(request(
            Some(json!(3)),
            "tools/call",
            Some(json!({ "name": "twin_start", "arguments": {} })),
        ))
        .expect("tool call response");
        assert_eq!(
            Some(true),
            response
                .result
                .as_ref()
                .and_then(|r| r["isError"].as_bool())
        );
        assert!(
            response.result.expect("tool result")["content"][0]["text"]
                .as_str()
                .expect("error body text")
                .contains("E_INTERNAL")
        );
    }

    #[test]
    fn initialize_advertises_server_identity_and_negotiated_version() {
        let response = dispatch(request(
            Some(json!(4)),
            "initialize",
            Some(json!({ "protocolVersion": "2024-11-05" })),
        ))
        .expect("initialize response");
        let result = response.result.expect("initialize result");
        assert_eq!("2024-11-05", result["protocolVersion"]);
        assert_eq!("twinning-mcp", result["serverInfo"]["name"]);
        assert_eq!(env!("CARGO_PKG_VERSION"), result["serverInfo"]["version"]);
        assert_eq!(twinning_mcp_core::SUPPORTED_MCP_PROTOCOL_VERSIONS.len(), 2);
    }
}
