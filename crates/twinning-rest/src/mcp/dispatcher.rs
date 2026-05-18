//! JSON-RPC 2.0 dispatcher for the MCP twin surface.

use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue, json};

use super::{
    catalog::{McpCatalog, McpPrompt, McpResource, McpTool},
    version::negotiate_initialize_protocol,
};

pub const JSONRPC_PARSE_ERROR: i64 = -32700;
pub const JSONRPC_INVALID_REQUEST: i64 = -32600;
pub const JSONRPC_METHOD_NOT_FOUND: i64 = -32601;
pub const JSONRPC_INVALID_PARAMS: i64 = -32602;
pub const JSONRPC_SERVER_ERROR: i64 = -32000;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Option<JsonValue>,
    #[serde(default)]
    pub id: Option<JsonValue>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    pub id: JsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<JsonValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolExecutability {
    Stubbable,
    UnsupportedShape,
}

pub fn dispatch(request: JsonRpcRequest, catalog: &McpCatalog) -> Option<JsonRpcResponse> {
    if request.id.is_none() && request.method.starts_with("notifications/") {
        return None;
    }

    let id = request.id.clone().unwrap_or(JsonValue::Null);
    if request.jsonrpc != "2.0" {
        return Some(error_response(
            id,
            JSONRPC_INVALID_REQUEST,
            "invalid JSON-RPC version",
            None,
        ));
    }

    Some(match request.method.as_str() {
        "initialize" => result_response(id, initialize_result(catalog, request.params.as_ref())),
        "tools/list" => result_response(id, tools_list_result(catalog)),
        "tools/call" => tools_call_response(id, request.params.as_ref(), catalog),
        "resources/list" => result_response(id, resources_list_result(catalog)),
        "resources/read" => resources_read_response(id, request.params.as_ref()),
        "prompts/list" => result_response(id, prompts_list_result(catalog)),
        "prompts/get" => prompts_get_response(id, request.params.as_ref(), catalog),
        _ => error_response(
            id,
            JSONRPC_METHOD_NOT_FOUND,
            format!("method not found: {}", request.method),
            None,
        ),
    })
}

pub fn classify_tool_executability(tool: &McpTool) -> ToolExecutability {
    if tool.output_schema.is_none() || is_stateful_tool_name(&tool.name) {
        return ToolExecutability::UnsupportedShape;
    }
    ToolExecutability::Stubbable
}

fn initialize_result(catalog: &McpCatalog, params: Option<&JsonValue>) -> JsonValue {
    let protocol = negotiate_initialize_protocol(params);
    json!({
        "protocolVersion": protocol.negotiated,
        "capabilities": {
            "tools": { "listChanged": false },
            "resources": { "listChanged": false, "subscribe": false },
            "prompts": { "listChanged": false }
        },
        "serverInfo": {
            "name": catalog.server_info.name,
            "version": catalog.server_info.version
        }
    })
}

fn tools_list_result(catalog: &McpCatalog) -> JsonValue {
    json!({
        "tools": catalog
            .tools
            .iter()
            .map(tool_list_entry)
            .collect::<Vec<_>>()
    })
}

fn tool_list_entry(tool: &McpTool) -> JsonValue {
    json!({
        "name": tool.name,
        "description": tool.description,
        "inputSchema": tool.input_schema
    })
}

fn tools_call_response(
    id: JsonValue,
    params: Option<&JsonValue>,
    catalog: &McpCatalog,
) -> JsonRpcResponse {
    let Some(name) = params
        .and_then(|params| params.get("name"))
        .and_then(JsonValue::as_str)
    else {
        return error_response(
            id,
            JSONRPC_INVALID_PARAMS,
            "tools/call params must include string field `name`",
            None,
        );
    };

    let Some(tool) = catalog.tools.iter().find(|tool| tool.name == name) else {
        return error_response(
            id,
            JSONRPC_INVALID_PARAMS,
            format!("unknown tool: {name}"),
            None,
        );
    };

    if classify_tool_executability(tool) == ToolExecutability::UnsupportedShape {
        return error_response(
            id,
            JSONRPC_SERVER_ERROR,
            "unsupported_shape",
            Some(json!({
                "kind": "unsupported_shape",
                "tool": name,
                "reason": "no output schema or stateful operation"
            })),
        );
    }

    let schema = tool
        .output_schema
        .as_ref()
        .expect("stubbable tool has an output schema");
    let stub = stub_from_schema(schema);
    let text = serde_json::to_string(&stub).unwrap_or_else(|_| "<stub>".to_owned());
    result_response(
        id,
        json!({
            "content": [
                {
                    "type": "text",
                    "text": text
                }
            ],
            "isError": false
        }),
    )
}

fn resources_list_result(catalog: &McpCatalog) -> JsonValue {
    json!({
        "resources": catalog
            .resources
            .iter()
            .map(resource_list_entry)
            .collect::<Vec<_>>()
    })
}

fn resource_list_entry(resource: &McpResource) -> JsonValue {
    let mut entry = JsonMap::new();
    if let Some(uri) = &resource.uri {
        entry.insert("uri".to_owned(), JsonValue::String(uri.clone()));
    }
    if let Some(uri_template) = &resource.uri_template {
        entry.insert(
            "uriTemplate".to_owned(),
            JsonValue::String(uri_template.clone()),
        );
    }
    entry.insert("name".to_owned(), JsonValue::String(resource.name.clone()));
    if let Some(mime_type) = &resource.mime_type {
        entry.insert("mimeType".to_owned(), JsonValue::String(mime_type.clone()));
    }
    JsonValue::Object(entry)
}

fn resources_read_response(id: JsonValue, params: Option<&JsonValue>) -> JsonRpcResponse {
    let Some(uri) = params
        .and_then(|params| params.get("uri"))
        .and_then(JsonValue::as_str)
    else {
        return error_response(
            id,
            JSONRPC_INVALID_PARAMS,
            "resources/read params must include string field `uri`",
            None,
        );
    };

    result_response(
        id,
        json!({
            "contents": [
                {
                    "uri": uri,
                    "text": "",
                    "mimeType": "text/plain"
                }
            ]
        }),
    )
}

fn prompts_list_result(catalog: &McpCatalog) -> JsonValue {
    json!({
        "prompts": catalog
            .prompts
            .iter()
            .map(prompt_list_entry)
            .collect::<Vec<_>>()
    })
}

fn prompt_list_entry(prompt: &McpPrompt) -> JsonValue {
    json!({
        "name": prompt.name,
        "description": prompt.description,
        "arguments": prompt.arguments
    })
}

fn prompts_get_response(
    id: JsonValue,
    params: Option<&JsonValue>,
    catalog: &McpCatalog,
) -> JsonRpcResponse {
    let Some(name) = params
        .and_then(|params| params.get("name"))
        .and_then(JsonValue::as_str)
    else {
        return error_response(
            id,
            JSONRPC_INVALID_PARAMS,
            "prompts/get params must include string field `name`",
            None,
        );
    };

    if !catalog.prompts.iter().any(|prompt| prompt.name == name) {
        return error_response(
            id,
            JSONRPC_INVALID_PARAMS,
            format!("unknown prompt: {name}"),
            None,
        );
    }

    result_response(
        id,
        json!({
            "messages": [
                {
                    "role": "user",
                    "content": {
                        "type": "text",
                        "text": format!("<stub prompt: {name}>")
                    }
                }
            ]
        }),
    )
}

fn result_response(id: JsonValue, result: JsonValue) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".to_owned(),
        id,
        result: Some(result),
        error: None,
    }
}

fn error_response(
    id: JsonValue,
    code: i64,
    message: impl Into<String>,
    data: Option<JsonValue>,
) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".to_owned(),
        id,
        result: None,
        error: Some(JsonRpcError {
            code,
            message: message.into(),
            data,
        }),
    }
}

fn is_stateful_tool_name(name: &str) -> bool {
    let lowercase = name.to_ascii_lowercase();
    ["write", "create", "delete", "update", "send", "post"]
        .iter()
        .any(|needle| lowercase.contains(needle))
}

fn stub_from_schema(schema: &JsonValue) -> JsonValue {
    if let Some(value) = schema.get("default") {
        return value.clone();
    }
    if let Some(value) = schema.get("const") {
        return value.clone();
    }
    if let Some(value) = schema
        .get("enum")
        .and_then(JsonValue::as_array)
        .and_then(|values| values.first())
    {
        return value.clone();
    }
    for combinator in ["oneOf", "anyOf", "allOf"] {
        if let Some(value) = schema
            .get(combinator)
            .and_then(JsonValue::as_array)
            .and_then(|schemas| schemas.first())
        {
            return stub_from_schema(value);
        }
    }

    match schema_type(schema).as_deref() {
        Some("object") => object_stub(schema),
        Some("array") => JsonValue::Array(Vec::new()),
        Some("integer" | "number") => json!(0),
        Some("boolean") => JsonValue::Bool(false),
        Some("null") => JsonValue::Null,
        Some("string") => JsonValue::String(String::new()),
        Some(_) | None if schema.get("properties").is_some() => object_stub(schema),
        Some(_) | None => JsonValue::Object(JsonMap::new()),
    }
}

fn schema_type(schema: &JsonValue) -> Option<String> {
    match schema.get("type") {
        Some(JsonValue::String(kind)) => Some(kind.clone()),
        Some(JsonValue::Array(kinds)) => kinds
            .iter()
            .filter_map(JsonValue::as_str)
            .find(|kind| *kind != "null")
            .map(str::to_owned),
        _ => None,
    }
}

fn object_stub(schema: &JsonValue) -> JsonValue {
    let mut object = JsonMap::new();
    if let Some(properties) = schema.get("properties").and_then(JsonValue::as_object) {
        for (name, property_schema) in properties {
            object.insert(name.clone(), stub_from_schema(property_schema));
        }
    }
    JsonValue::Object(object)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        JSONRPC_INVALID_PARAMS, JSONRPC_METHOD_NOT_FOUND, JSONRPC_SERVER_ERROR, JsonRpcRequest,
        ToolExecutability, classify_tool_executability, dispatch,
    };
    use crate::mcp::catalog::{
        McpCatalog, McpCatalogSource, McpPrompt, McpPromptArgument, McpResource, McpServerInfo,
        McpTool,
    };

    #[test]
    fn initialize_returns_capabilities_and_server_info() {
        let response = dispatch(
            request(
                1,
                "initialize",
                Some(json!({ "protocolVersion": "2024-11-05" })),
            ),
            &catalog(),
        )
        .expect("response");
        let result = response.result.expect("result");

        assert_eq!("2024-11-05", result["protocolVersion"]);
        assert_eq!("demo", result["serverInfo"]["name"]);
        assert_eq!(false, result["capabilities"]["tools"]["listChanged"]);
    }

    #[test]
    fn initialize_negotiates_unsupported_client_protocol_to_latest_supported() {
        let response = dispatch(
            request(
                11,
                "initialize",
                Some(json!({ "protocolVersion": "2099-01-01" })),
            ),
            &catalog(),
        )
        .expect("response");
        let result = response.result.expect("result");

        assert_eq!("2025-03-26", result["protocolVersion"]);
    }

    #[test]
    fn tools_list_returns_catalog_tools() {
        let response = dispatch(request(2, "tools/list", None), &catalog()).expect("response");
        let tools = response.result.expect("result")["tools"]
            .as_array()
            .expect("tools array")
            .clone();

        assert_eq!(2, tools.len());
        assert_eq!("lookup", tools[0]["name"]);
        assert_eq!("object", tools[0]["inputSchema"]["type"]);
    }

    #[test]
    fn tools_call_with_output_schema_returns_typed_stub() {
        let response = dispatch(
            request(3, "tools/call", Some(json!({ "name": "lookup" }))),
            &catalog(),
        )
        .expect("response");

        let result = response.result.expect("result");
        assert_eq!(false, result["isError"]);
        assert_eq!(
            r#"{"count":0,"items":[],"ok":false,"title":""}"#,
            result["content"][0]["text"]
        );
    }

    #[test]
    fn tools_call_without_output_schema_returns_unsupported_shape() {
        let response = dispatch(
            request(4, "tools/call", Some(json!({ "name": "read_freeform" }))),
            &catalog(),
        )
        .expect("response");

        let error = response.error.expect("error");
        assert_eq!(JSONRPC_SERVER_ERROR, error.code);
        assert_eq!("unsupported_shape", error.data.expect("data")["kind"]);
    }

    #[test]
    fn unknown_tool_returns_invalid_params() {
        let response = dispatch(
            request(5, "tools/call", Some(json!({ "name": "missing" }))),
            &catalog(),
        )
        .expect("response");

        assert_eq!(JSONRPC_INVALID_PARAMS, response.error.expect("error").code);
    }

    #[test]
    fn resources_list_and_read_return_stub_contents() {
        let response = dispatch(request(6, "resources/list", None), &catalog()).expect("response");
        assert_eq!(
            "file:///tmp/demo",
            response.result.expect("result")["resources"][0]["uri"]
        );

        let response = dispatch(
            request(
                7,
                "resources/read",
                Some(json!({ "uri": "file:///tmp/demo" })),
            ),
            &catalog(),
        )
        .expect("response");
        assert_eq!("", response.result.expect("result")["contents"][0]["text"]);
    }

    #[test]
    fn prompts_list_and_get_return_stub_prompt() {
        let response = dispatch(request(8, "prompts/list", None), &catalog()).expect("response");
        assert_eq!(
            "summarize",
            response.result.expect("result")["prompts"][0]["name"]
        );

        let response = dispatch(
            request(9, "prompts/get", Some(json!({ "name": "summarize" }))),
            &catalog(),
        )
        .expect("response");
        assert_eq!(
            "<stub prompt: summarize>",
            response.result.expect("result")["messages"][0]["content"]["text"]
        );
    }

    #[test]
    fn unknown_method_returns_method_not_found() {
        let response = dispatch(request(10, "not/a-method", None), &catalog()).expect("response");

        assert_eq!(
            JSONRPC_METHOD_NOT_FOUND,
            response.error.expect("error").code
        );
    }

    #[test]
    fn initialized_notification_returns_no_response() {
        let response = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_owned(),
                method: "notifications/initialized".to_owned(),
                params: None,
                id: None,
            },
            &catalog(),
        );

        assert!(response.is_none());
    }

    #[test]
    fn tool_executability_requires_output_schema_and_non_stateful_name() {
        let catalog = catalog();

        assert_eq!(
            ToolExecutability::Stubbable,
            classify_tool_executability(&catalog.tools[0])
        );
        assert_eq!(
            ToolExecutability::UnsupportedShape,
            classify_tool_executability(&catalog.tools[1])
        );

        let mut stateful_with_schema = catalog.tools[0].clone();
        stateful_with_schema.name = "create_lookup".to_owned();
        assert_eq!(
            ToolExecutability::UnsupportedShape,
            classify_tool_executability(&stateful_with_schema)
        );
    }

    fn request(id: i64, method: &str, params: Option<serde_json::Value>) -> JsonRpcRequest {
        JsonRpcRequest {
            jsonrpc: "2.0".to_owned(),
            method: method.to_owned(),
            params,
            id: Some(json!(id)),
        }
    }

    fn catalog() -> McpCatalog {
        McpCatalog {
            server_info: McpServerInfo {
                name: "demo".to_owned(),
                version: "1.0.0".to_owned(),
            },
            tools: vec![
                McpTool {
                    name: "lookup".to_owned(),
                    description: Some("Lookup a value".to_owned()),
                    input_schema: json!({ "type": "object" }),
                    output_schema: Some(json!({
                        "type": "object",
                        "properties": {
                            "count": { "type": "integer" },
                            "items": { "type": "array", "items": { "type": "string" } },
                            "ok": { "type": "boolean" },
                            "title": { "type": "string" }
                        }
                    })),
                },
                McpTool {
                    name: "read_freeform".to_owned(),
                    description: None,
                    input_schema: json!({ "type": "object" }),
                    output_schema: None,
                },
            ],
            resources: vec![McpResource {
                uri: Some("file:///tmp/demo".to_owned()),
                uri_template: None,
                name: "demo".to_owned(),
                mime_type: Some("text/plain".to_owned()),
            }],
            prompts: vec![McpPrompt {
                name: "summarize".to_owned(),
                description: Some("Summarize input".to_owned()),
                arguments: vec![McpPromptArgument {
                    name: "topic".to_owned(),
                    description: None,
                    required: Some(true),
                }],
            }],
            security_schemes: Vec::new(),
            required_auth_schemes: Vec::new(),
            catalog_hash: "sha256:test".to_owned(),
            source: McpCatalogSource::Manifest {
                path: "mcp.json".to_owned(),
            },
        }
    }
}
