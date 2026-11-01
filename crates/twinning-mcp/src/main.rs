#[cfg(not(unix))]
compile_error!("twinning-mcp supports Unix only");

mod cli;
mod errors;
mod server;
mod tools;

fn main() {
    std::process::exit(cli::run());
}
