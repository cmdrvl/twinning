//! Shared MCP wire types and newline-delimited stdio transport.

pub mod jsonrpc;
pub mod stdio;
pub mod version;

pub use jsonrpc::{
    JSONRPC_INVALID_PARAMS, JSONRPC_INVALID_REQUEST, JSONRPC_METHOD_NOT_FOUND, JSONRPC_PARSE_ERROR,
    JSONRPC_SERVER_ERROR,
};
pub use jsonrpc::{JsonRpcError, JsonRpcRequest, JsonRpcResponse};
pub use version::{
    McpProtocolNegotiation, SUPPORTED_MCP_PROTOCOL_VERSIONS, negotiate_initialize_protocol,
};
