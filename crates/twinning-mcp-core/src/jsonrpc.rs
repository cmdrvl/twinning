//! JSON-RPC 2.0 request, response, and error types.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

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

pub fn parse_error(detail: impl Into<String>) -> JsonRpcError {
    JsonRpcError {
        code: JSONRPC_PARSE_ERROR,
        message: "parse error".to_owned(),
        data: Some(serde_json::json!({ "detail": detail.into() })),
    }
}

pub fn invalid_request(detail: impl Into<String>) -> JsonRpcError {
    JsonRpcError {
        code: JSONRPC_INVALID_REQUEST,
        message: "invalid request".to_owned(),
        data: Some(serde_json::json!({ "detail": detail.into() })),
    }
}

pub fn result_response(id: JsonValue, result: JsonValue) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".to_owned(),
        id,
        result: Some(result),
        error: None,
    }
}

pub fn error_response(
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

/// Parses JSON separately from the request shape so malformed JSON and a valid
/// JSON value that is not a request receive their distinct JSON-RPC errors.
pub fn parse_request(line: &str) -> Result<JsonRpcRequest, JsonRpcError> {
    let value: JsonValue =
        serde_json::from_str(line).map_err(|error| parse_error(error.to_string()))?;
    serde_json::from_value(value).map_err(|error| invalid_request(error.to_string()))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        JSONRPC_INVALID_REQUEST, JSONRPC_PARSE_ERROR, JsonRpcRequest, error_response,
        parse_request, result_response,
    };

    #[test]
    fn jsonrpc_response_shapes_omit_absent_fields() {
        let result = result_response(json!(7), json!({ "ok": true }));
        let encoded = serde_json::to_value(result).expect("response serializes");
        assert_eq!(
            json!({"jsonrpc":"2.0","id":7,"result":{"ok":true}}),
            encoded
        );

        let error = error_response(json!(null), -32601, "unknown method", None);
        let encoded = serde_json::to_value(error).expect("error response serializes");
        assert_eq!(
            json!({"jsonrpc":"2.0","id":null,"error":{"code":-32601,"message":"unknown method"}}),
            encoded
        );
    }

    #[test]
    fn malformed_json_and_invalid_requests_have_distinct_codes() {
        assert_eq!(
            JSONRPC_PARSE_ERROR,
            parse_request("{").expect_err("invalid JSON").code
        );
        assert_eq!(
            JSONRPC_INVALID_REQUEST,
            parse_request("{\"jsonrpc\":\"2.0\"}")
                .expect_err("missing method")
                .code
        );
    }

    #[test]
    fn request_accepts_optional_params_and_id() {
        let request =
            parse_request(r#"{"jsonrpc":"2.0","method":"ping"}"#).expect("notification parses");
        assert_eq!("ping", request.method);
        assert_eq!(None, request.id);
        assert_eq!(None, request.params);

        assert_eq!("2.0", request.jsonrpc);
        let _: JsonRpcRequest = request;
    }
}
