#![forbid(unsafe_code)]
#![cfg(any(feature = "postgres", feature = "mcp", feature = "snowflake"))]

#[path = "support.rs"]
pub(crate) mod support;

#[path = "canaries/mod.rs"]
mod canaries;
