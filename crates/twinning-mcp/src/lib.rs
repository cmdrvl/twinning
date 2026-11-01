//! Reusable supervisor core. The MCP tool handlers are wired in the M4 layer.

pub mod argv;
pub mod errors;
pub mod locate;
pub mod log;
pub mod paths;
pub mod registry;
pub mod stale;
pub mod supervisor;

#[cfg(test)]
#[path = "../tests/common/mod.rs"]
pub mod test_common;
