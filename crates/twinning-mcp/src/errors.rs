//! Tool error body and the stable error-code table used by capabilities.

use serde_json::{Value, json};

pub const ERROR_CODES: &[&str] = &[
    "E_ARGUMENT_INVALID",
    "E_KIND_UNSUPPORTED",
    "E_TWINNING_NOT_FOUND",
    "E_WORKDIR_UNSAFE",
    "E_PATH_NOT_FOUND",
    "E_PATH_OUTSIDE_WORKDIR",
    "E_TWIN_CAP_REACHED",
    "E_TWIN_NOT_FOUND",
    "E_START_FAILED",
    "E_READY_TIMEOUT",
    "E_STOP_TIMEOUT",
    "E_REPORT_NOT_READY",
    "E_REPORT_UNREADABLE",
    "E_SEED_CONTRACT_FAILED",
    "E_INTERNAL",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolError {
    pub code: &'static str,
    pub message: String,
    pub hint: &'static str,
}

impl ToolError {
    pub fn not_implemented(tool: &str) -> Self {
        Self {
            code: "E_INTERNAL",
            message: format!("{tool}: not implemented"),
            hint: "Run twinning-mcp doctor",
        }
    }

    pub fn body(&self) -> Value {
        json!({
            "error": {
                "code": self.code,
                "message": self.message,
                "hint": self.hint
            }
        })
    }

    pub fn mcp_result(&self) -> Value {
        let text = serde_json::to_string(&self.body())
            .expect("serializing a fixed tool error body cannot fail");
        json!({
            "content": [{ "type": "text", "text": text }],
            "isError": true
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::ToolError;

    #[test]
    fn not_implemented_tool_error_uses_d11_body_and_hint() {
        let error = ToolError::not_implemented("twin_start");
        assert_eq!(
            json!({"error":{"code":"E_INTERNAL","message":"twin_start: not implemented","hint":"Run twinning-mcp doctor"}}),
            error.body()
        );
        assert_eq!(true, error.mcp_result()["isError"]);
    }
}
