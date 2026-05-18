#![forbid(unsafe_code)]

use clap::Parser;

pub mod cli;
pub mod config;
pub mod doctor;
pub mod migration_proof;
pub mod orchestration_manifest;
#[cfg(feature = "rest")]
pub mod port;
pub mod protocol;
pub mod refusal;
pub mod report;
pub mod runtime;

pub use twinning_kernel::{
    backend, catalog, declaration, ir, kernel, materialize, query_trace, result, snapshot,
    verify_bridge,
};

const OPERATOR_JSON: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/operator.json"));

pub fn run() -> Result<u8, Box<dyn std::error::Error>> {
    let cli = match cli::Cli::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            let exit_code = match error.kind() {
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion => 0,
                _ => 2,
            };
            error.print()?;
            return Ok(exit_code);
        }
    };

    if cli.describe {
        print!("{OPERATOR_JSON}");
        return Ok(0);
    }

    if let Some(cli::Command::Doctor(args)) = &cli.command {
        let execution = doctor::execute(args, cli.json)?;
        print!("{}", execution.stdout);
        return Ok(execution.exit_code);
    }

    if let Some(cli::Command::Proof(args)) = &cli.command {
        let execution = migration_proof::execute(args, cli.json)?;
        print!("{}", execution.stdout);
        return Ok(execution.exit_code);
    }

    #[cfg(feature = "rest")]
    if let Some(cli::Command::Port(args)) = &cli.command {
        let execution = port::execute(args, cli.json)?;
        print!("{}", execution.stdout);
        return Ok(execution.exit_code);
    }

    #[cfg(feature = "rest")]
    if let Some(cli::Command::Rest(_args)) = &cli.command {
        let config = match config::rest_config_from_cli(&cli) {
            Ok(config) => config,
            Err(refusal) => {
                print!("{}", refusal.render(cli.json)?);
                return Ok(2);
            }
        };

        let execution = protocol::rest::listener::run(config)?;
        print!("{}", execution.stdout);
        return Ok(execution.exit_code);
    }

    #[cfg(feature = "mcp")]
    if let Some(cli::Command::Mcp(_args)) = &cli.command {
        let config = match config::mcp_config_from_cli(&cli) {
            Ok(config) => config,
            Err(refusal) => {
                print!("{}", refusal.render(cli.json)?);
                return Ok(2);
            }
        };

        let execution = if config.stdio {
            protocol::rest::mcp::stdio::run_stdio(config)?
        } else {
            protocol::rest::mcp::listener::run(config)?
        };
        print!("{}", execution.stdout);
        return Ok(execution.exit_code);
    }

    #[cfg(feature = "snowflake")]
    if let Some(cli::Command::Snowflake(_args)) = &cli.command {
        let config = match config::snowflake_config_from_cli(&cli) {
            Ok(config) => config,
            Err(refusal) => {
                print!("{}", refusal.render(cli.json)?);
                return Ok(2);
            }
        };

        let execution = twinning_snowflake::listener::run(config)?;
        print!("{}", execution.stdout);
        return Ok(execution.exit_code);
    }

    let config = match config::twin_config_from_cli(&cli) {
        Ok(config) => config,
        Err(refusal) => {
            print!("{}", refusal.render(cli.json)?);
            return Ok(2);
        }
    };

    let execution = runtime::execute(config)?;
    print!("{}", execution.stdout);
    Ok(execution.exit_code)
}
