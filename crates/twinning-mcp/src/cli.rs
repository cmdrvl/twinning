//! CLI parsing, usage, capabilities output, and stable exit codes.

use std::{
    env,
    ffi::OsString,
    io::{self, IsTerminal, Write},
    path::PathBuf,
};

use clap::{
    ColorChoice, CommandFactory, FromArgMatches, Parser, Subcommand,
    error::{ContextKind, ContextValue, ErrorKind},
};
use serde_json::{Value, json};
use twinning_mcp_core::SUPPORTED_MCP_PROTOCOL_VERSIONS;

use crate::{errors::ERROR_CODES, server, tools::tool_names};

pub const EXIT_OK: i32 = 0;
pub const EXIT_USER_ERROR: i32 = 1;
pub const EXIT_SAFETY_BLOCK: i32 = 2;
pub const EXIT_ENVIRONMENT_ERROR: i32 = 3;
pub const EXIT_UPSTREAM_FAILURE: i32 = 4;

const EXIT_CODE_MEANINGS: &[(i32, &str)] = &[
    (EXIT_OK, "OK"),
    (EXIT_USER_ERROR, "User input error"),
    (EXIT_SAFETY_BLOCK, "Safety block"),
    (EXIT_ENVIRONMENT_ERROR, "Environment error"),
    (EXIT_UPSTREAM_FAILURE, "Upstream or twin failure"),
];

#[derive(Debug, Parser)]
#[command(name = "twinning-mcp", version, disable_help_subcommand = true)]
struct Cli {
    #[arg(long, global = true, value_name = "DIR")]
    workdir: Option<PathBuf>,

    #[arg(long, global = true, value_name = "PATH")]
    twinning_bin: Option<PathBuf>,

    #[arg(long, global = true)]
    restricted: bool,

    #[arg(long, global = true, value_name = "COUNT")]
    max_twins: Option<usize>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Configure a supported MCP client.
    Setup {
        #[arg(long)]
        json: bool,
        #[arg(long)]
        dry_run: bool,
    },
    /// Check the twinning binary and work directory.
    Doctor {
        #[arg(long)]
        json: bool,
    },
    /// Print this server's capabilities.
    Capabilities {
        #[arg(long)]
        json: bool,
    },
}

pub fn run() -> i32 {
    let args: Vec<OsString> = env::args_os().collect();
    let command = Cli::command().color(ColorChoice::Never);
    let matches = match command.try_get_matches_from(args.clone()) {
        Ok(matches) => matches,
        Err(error) => return report_parse_error(error, &args),
    };
    let cli = match Cli::from_arg_matches(&matches) {
        Ok(cli) => cli,
        Err(error) => return report_parse_error(error, &args),
    };

    let _server_options = (cli.workdir, cli.twinning_bin, cli.restricted, cli.max_twins);

    match cli.command {
        None if assume_tty() => {
            print_usage();
            EXIT_OK
        }
        None => match server::serve() {
            Ok(()) => EXIT_OK,
            Err(error) => {
                eprintln!("error: twinning-mcp stdio failed: {error}");
                eprintln!("next: twinning-mcp doctor");
                EXIT_ENVIRONMENT_ERROR
            }
        },
        Some(Commands::Capabilities { json: _ }) => {
            print_capabilities();
            EXIT_OK
        }
        Some(Commands::Setup {
            json: _,
            dry_run: _,
        }) => {
            eprintln!("error: setup is not yet implemented");
            eprintln!("next: twinning-mcp setup");
            EXIT_USER_ERROR
        }
        Some(Commands::Doctor { json: _ }) => {
            eprintln!("error: doctor is not yet implemented");
            eprintln!("next: twinning-mcp doctor");
            EXIT_USER_ERROR
        }
    }
}

fn assume_tty() -> bool {
    env::var("TWINNING_MCP_ASSUME_TTY").is_ok_and(|value| value == "1") || io::stdin().is_terminal()
}

fn print_usage() {
    println!(
        "Usage: twinning-mcp [OPTIONS] [COMMAND]\n\nCommands:\n  setup         Configure a supported MCP client\n  doctor        Check the twinning binary and work directory\n  capabilities  Print this server's capabilities\n\nOptions:\n  --workdir <DIR>       Set the working directory\n  --twinning-bin <PATH>  Select the twinning executable\n  --restricted          Refuse seed, restore, and verify paths\n  --max-twins <COUNT>   Set the maximum number of live twins\n  -h, --help            Print help\n  -V, --version         Print version\n\nnext: twinning-mcp setup\nnext: twinning-mcp doctor"
    );
}

pub fn capabilities() -> Value {
    json!({
        "name": "twinning-mcp",
        "version": env!("CARGO_PKG_VERSION"),
        "protocol_versions": SUPPORTED_MCP_PROTOCOL_VERSIONS,
        "kinds": ["postgres"],
        "subcommands": ["setup", "doctor", "capabilities"],
        "tools": tool_names(),
        "error_codes": ERROR_CODES,
        "exit_codes": EXIT_CODE_MEANINGS.iter().map(|(code, meaning)| {
            json!({ "code": code, "meaning": meaning })
        }).collect::<Vec<_>>()
    })
}

fn print_capabilities() {
    match serde_json::to_string(&capabilities()) {
        Ok(rendered) => println!("{rendered}"),
        Err(error) => {
            eprintln!("error: could not render capabilities: {error}");
            eprintln!("next: twinning-mcp --help");
        }
    }
}

fn report_parse_error(error: clap::Error, args: &[OsString]) -> i32 {
    match error.kind() {
        ErrorKind::DisplayHelp => {
            print!("{error}");
            let _ = io::stdout().flush();
            return EXIT_OK;
        }
        ErrorKind::DisplayVersion => {
            print!("{error}");
            let _ = io::stdout().flush();
            return EXIT_OK;
        }
        _ => {}
    }

    if let Some((message, next)) = corrected_command(&error, args) {
        eprintln!("{message}");
        eprintln!("next: {next}");
    } else {
        eprintln!("error: invalid command line");
        eprintln!("next: twinning-mcp --help");
    }
    EXIT_USER_ERROR
}

fn corrected_command(error: &clap::Error, args: &[OsString]) -> Option<(String, String)> {
    if let Some(invalid) = context_string(error, ContextKind::InvalidSubcommand)
        && let Some(suggestion) = context_first_suggestion(error, ContextKind::SuggestedSubcommand)
    {
        return Some((
            format!(
                "error: unknown subcommand '{invalid}'. Did you mean: twinning-mcp {suggestion}"
            ),
            format!("twinning-mcp {suggestion}"),
        ));
    }

    if let Some(invalid) = context_string(error, ContextKind::InvalidArg)
        && let Some(suggestion) = context_first_suggestion(error, ContextKind::SuggestedArg)
    {
        let tokens = args
            .iter()
            .skip(1)
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let mut corrected = Vec::with_capacity(tokens.len());
        let mut replaced = false;
        for token in tokens {
            if !replaced && token == invalid {
                corrected.push(suggestion.clone());
                replaced = true;
            } else {
                corrected.push(token);
            }
        }
        if replaced {
            let command = format!("twinning-mcp {}", corrected.join(" "));
            return Some((
                format!("error: unknown flag '{invalid}'. Did you mean: {command}"),
                command,
            ));
        }
    }

    None
}

fn context_string(error: &clap::Error, key: ContextKind) -> Option<String> {
    match error.get(key)? {
        ContextValue::String(value) => Some(value.clone()),
        ContextValue::StyledStr(value) => Some(value.to_string()),
        _ => None,
    }
}

fn context_first_suggestion(error: &clap::Error, key: ContextKind) -> Option<String> {
    match error.get(key)? {
        ContextValue::Strings(values) => values.first().cloned(),
        ContextValue::StyledStrs(values) => values.first().map(ToString::to_string),
        ContextValue::String(value) => Some(value.clone()),
        ContextValue::StyledStr(value) => Some(value.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        EXIT_ENVIRONMENT_ERROR, EXIT_OK, EXIT_SAFETY_BLOCK, EXIT_UPSTREAM_FAILURE, EXIT_USER_ERROR,
        capabilities,
    };

    #[test]
    fn exit_code_constants_cover_the_specified_cli_domain() {
        assert_eq!(
            [0, 1, 2, 3, 4],
            [
                EXIT_OK,
                EXIT_USER_ERROR,
                EXIT_SAFETY_BLOCK,
                EXIT_ENVIRONMENT_ERROR,
                EXIT_UPSTREAM_FAILURE,
            ]
        );
    }

    #[test]
    fn capabilities_are_built_from_the_server_tool_and_error_tables() {
        let capabilities = capabilities();
        assert_eq!("twinning-mcp", capabilities["name"]);
        assert_eq!(env!("CARGO_PKG_VERSION"), capabilities["version"]);
        assert_eq!("twin_doctor", capabilities["tools"][0]);
        assert_eq!("E_ARGUMENT_INVALID", capabilities["error_codes"][0]);
        assert_eq!(json!(4), capabilities["exit_codes"][4]["code"]);
    }
}
