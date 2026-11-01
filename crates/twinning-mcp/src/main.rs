#[cfg(not(unix))]
compile_error!("twinning-mcp supports Unix only");

mod cli;
pub use twinning_mcp::errors;
use twinning_mcp::{argv, locate, log, paths, registry, stale, supervisor};
mod doctor;
mod report;
mod server;
mod tools;

fn main() {
    std::process::exit(cli::run());
}
