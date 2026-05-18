//! MCP protocol version negotiation.

use serde_json::Value as JsonValue;

pub const SUPPORTED_MCP_PROTOCOL_VERSIONS: &[&str] = &["2024-11-05", "2025-03-26"];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpProtocolNegotiation {
    pub requested: Option<String>,
    pub negotiated: String,
    pub supported: bool,
}

pub fn negotiate_protocol_version(client_version: &str) -> &'static str {
    SUPPORTED_MCP_PROTOCOL_VERSIONS
        .iter()
        .find(|version| **version == client_version)
        .copied()
        .unwrap_or_else(default_protocol_version)
}

pub fn default_protocol_version() -> &'static str {
    SUPPORTED_MCP_PROTOCOL_VERSIONS
        .last()
        .copied()
        .expect("MCP supported protocol versions are checked in")
}

pub fn negotiate_initialize_protocol(params: Option<&JsonValue>) -> McpProtocolNegotiation {
    let requested = params
        .and_then(|params| params.get("protocolVersion"))
        .and_then(JsonValue::as_str)
        .map(str::to_owned);
    let negotiated = requested
        .as_deref()
        .map_or_else(default_protocol_version, negotiate_protocol_version)
        .to_owned();
    let supported = requested
        .as_deref()
        .is_none_or(|requested| negotiated == requested);

    McpProtocolNegotiation {
        requested,
        negotiated,
        supported,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        default_protocol_version, negotiate_initialize_protocol, negotiate_protocol_version,
    };

    #[test]
    fn supported_versions_are_echoed_and_unknown_versions_fall_forward() {
        assert_eq!("2024-11-05", negotiate_protocol_version("2024-11-05"));
        assert_eq!(
            default_protocol_version(),
            negotiate_protocol_version("2099-01-01")
        );

        let supported = negotiate_initialize_protocol(Some(&json!({
            "protocolVersion": "2025-03-26"
        })));
        assert_eq!(Some("2025-03-26".to_owned()), supported.requested);
        assert_eq!("2025-03-26", supported.negotiated);
        assert!(supported.supported);

        let unsupported = negotiate_initialize_protocol(Some(&json!({
            "protocolVersion": "2099-01-01"
        })));
        assert_eq!(Some("2099-01-01".to_owned()), unsupported.requested);
        assert_eq!(default_protocol_version(), unsupported.negotiated);
        assert!(!unsupported.supported);
    }
}
