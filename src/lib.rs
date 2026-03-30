#![forbid(unsafe_code)]

use clap::Parser;

pub mod backend;
pub mod catalog;
pub mod cli;
pub mod config;
pub mod ir;
pub mod kernel;
pub mod protocol;
pub mod refusal;
pub mod report;
pub mod result;
pub mod runtime;
pub mod snapshot;
pub mod verify_bridge;

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

    let config = match config::TwinConfig::from_cli(&cli) {
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
