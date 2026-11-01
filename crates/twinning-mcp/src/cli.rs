//! CLI parsing, usage, capabilities output, and stable exit codes.

use std::{
    env,
    ffi::OsString,
    io::{self, IsTerminal, Write},
    path::Path,
    path::PathBuf,
};

use clap::{
    ColorChoice, CommandFactory, FromArgMatches, Parser, Subcommand,
    error::{ContextKind, ContextValue, ErrorKind},
};
use serde_json::{Value, json};
use twinning_mcp_core::SUPPORTED_MCP_PROTOCOL_VERSIONS;

use crate::{doctor, errors::ERROR_CODES, paths::Workdir, server, setup, tools::tool_names};

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
        #[arg(long, value_name = "CLIENT")]
        client: Option<String>,
        #[arg(long)]
        json: bool,
        #[arg(long)]
        dry_run: bool,
        #[arg(long)]
        print: bool,
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

    match cli.command {
        None if assume_tty() => {
            print_usage();
            EXIT_OK
        }
        None => match server::serve(server::ServerOptions {
            workdir: cli.workdir,
            twinning_bin: cli.twinning_bin,
            restricted: cli.restricted,
            max_twins: cli.max_twins,
        }) {
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
            client,
            json,
            dry_run,
            print,
        }) => run_setup(
            client.as_deref(),
            cli.workdir.as_deref(),
            json,
            dry_run,
            print,
        ),
        Some(Commands::Doctor { json }) => run_doctor(
            cli.workdir.as_deref(),
            cli.twinning_bin.as_deref(),
            cli.restricted,
            cli.max_twins.unwrap_or(5),
            json,
        ),
    }
}

fn run_setup(
    client: Option<&str>,
    workdir: Option<&Path>,
    json_output: bool,
    dry_run: bool,
    print: bool,
) -> i32 {
    let launch_dir = match env::current_dir() {
        Ok(path) => path,
        Err(error) => {
            eprintln!("error: setup: cannot read launch directory: {error}");
            eprintln!("next: twinning-mcp doctor");
            return EXIT_ENVIRONMENT_ERROR;
        }
    };
    let home = env::var_os("HOME").map(PathBuf::from);
    let executable = match env::current_exe() {
        Ok(path) => path,
        Err(error) => {
            eprintln!("error: setup: cannot locate the running executable: {error}");
            eprintln!("next: twinning-mcp doctor");
            return EXIT_ENVIRONMENT_ERROR;
        }
    };
    let result = match setup::configure(
        client,
        workdir,
        &launch_dir,
        home.as_deref(),
        &executable,
        dry_run,
        print,
    ) {
        Ok(result) => result,
        Err(error) => {
            eprintln!("error: {}", error.message);
            eprintln!("next: {}", error.next);
            return match error.kind {
                setup::FailureKind::User => EXIT_USER_ERROR,
                setup::FailureKind::Safety => EXIT_SAFETY_BLOCK,
                setup::FailureKind::Environment => EXIT_ENVIRONMENT_ERROR,
            };
        }
    };

    if json_output {
        if let Some(diff) = &result.diff {
            eprint!("{diff}");
        }
        let output = json!({
            "client": result.client,
            "config_path": result.config_path,
            "action": result.action,
            "backup_path": result.backup_path,
            "restart_required": result.restart_required
        });
        println!("{output}");
        return EXIT_OK;
    }
    if result.action == "no_change" {
        println!("no change");
        return EXIT_OK;
    }
    if result.action == "printed" {
        match serde_json::to_string_pretty(&result.snippet) {
            Ok(snippet) => println!("{snippet}"),
            Err(error) => {
                eprintln!("error: setup: could not render config snippet: {error}");
                eprintln!("next: twinning-mcp doctor");
                return EXIT_ENVIRONMENT_ERROR;
            }
        }
        return EXIT_OK;
    }
    if let Some(diff) = result.diff {
        print!("{diff}");
    }
    println!("action: {}", result.action);
    println!("client: {}", setup::client_name(&result.client));
    if let Some(path) = &result.config_path {
        println!("config_path: {}", path.display());
    }
    match &result.backup_path {
        Some(path) => println!("backup_path: {}", path.display()),
        None => println!("backup_path: none"),
    }
    println!("restart_required: {}", result.restart_required);
    if result.restart_required {
        println!(
            "Restart {} to load the server.",
            setup::client_name(&result.client)
        );
    } else if result.action == "dry_run" {
        println!("No file was written.");
    }
    EXIT_OK
}

fn run_doctor(
    explicit_workdir: Option<&Path>,
    explicit_bin: Option<&Path>,
    restricted: bool,
    cap: usize,
    json_output: bool,
) -> i32 {
    let launch_dir = match env::current_dir() {
        Ok(path) => path,
        Err(error) => {
            eprintln!("error: doctor: cannot read launch directory: {error}");
            eprintln!("next: twinning-mcp doctor --workdir <existing-directory>");
            return EXIT_ENVIRONMENT_ERROR;
        }
    };
    let home = env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/"));
    let env_workdir = env::var_os("TWINNING_MCP_WORKDIR").map(PathBuf::from);
    let workdir = Workdir::resolve(explicit_workdir, env_workdir.as_deref(), &launch_dir, &home);
    let env_bin = env::var_os("TWINNING_BIN").map(PathBuf::from);
    let explicit_binary_missing = explicit_bin.is_some_and(|path| !path.exists());
    let diagnostic = if explicit_binary_missing {
        let workdir_status = match &workdir {
            Ok(workdir) => json!({
                "path": workdir.path,
                "safe": !workdir.unsafe_default,
                "explicit": workdir.explicit
            }),
            Err(error) => json!({
                "path": Value::Null,
                "safe": false,
                "error": error.code
            }),
        };
        json!({
            "twinning_bin": Value::Null,
            "twinning_version": Value::Null,
            "health": "unavailable",
            "capabilities": [],
            "workdir": workdir_status,
            "cap": cap,
            "restricted": restricted
        })
    } else {
        doctor::diagnose(explicit_bin, env_bin.as_deref(), &workdir, cap, restricted)
    };
    let binary = diagnostic["twinning_bin"].as_str();
    let health_status = diagnostic["health"]["status"].as_str();
    let health_ok = health_status == Some("healthy");
    let workdir_safe = diagnostic["workdir"]["safe"].as_bool() == Some(true);
    let binary_ok = binary.is_some();
    let workdir_detail = diagnostic["workdir"]["path"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| diagnostic["workdir"]["error"].as_str().map(str::to_owned))
        .unwrap_or_else(|| "workdir unavailable".to_owned());
    let binary_detail = binary
        .map(str::to_owned)
        .unwrap_or_else(|| "twinning executable unavailable".to_owned());
    let health_detail = if let Some(status) = health_status {
        format!(
            "{status} ({})",
            diagnostic["health"]["outcome"]
                .as_str()
                .unwrap_or("unknown")
        )
    } else {
        diagnostic["health"]
            .as_str()
            .unwrap_or("unavailable")
            .to_owned()
    };
    let checks = json!([
        {
            "name": "twinning_binary",
            "ok": binary_ok,
            "detail": binary_detail,
            "next": "set TWINNING_BIN or pass --twinning-bin <path>"
        },
        {
            "name": "health",
            "ok": health_ok,
            "detail": health_detail,
            "next": "twinning doctor health --json"
        },
        {
            "name": "workdir",
            "ok": workdir_safe,
            "detail": workdir_detail,
            "next": "twinning-mcp doctor --workdir <safe-directory>"
        }
    ]);
    let mut report = diagnostic.as_object().cloned().unwrap_or_default();
    report.insert("checks".into(), checks.clone());
    let report = Value::Object(report);
    let exit_code = if !workdir_safe {
        EXIT_SAFETY_BLOCK
    } else if !binary_ok {
        if !explicit_binary_missing && selected_binary_exists(explicit_bin, env_bin.as_deref()) {
            EXIT_UPSTREAM_FAILURE
        } else {
            EXIT_ENVIRONMENT_ERROR
        }
    } else if !health_ok {
        EXIT_UPSTREAM_FAILURE
    } else {
        EXIT_OK
    };

    if json_output {
        println!("{report}");
    } else {
        println!("twinning-mcp doctor");
        for check in checks.as_array().expect("checks is always an array") {
            let status = if check["ok"] == true { "ok" } else { "failed" };
            println!(
                "{}: {} ({})",
                check["name"].as_str().unwrap_or("check"),
                status,
                check["detail"].as_str().unwrap_or("unknown")
            );
        }
    }
    if exit_code != EXIT_OK {
        let (message, next) = match exit_code {
            EXIT_SAFETY_BLOCK => (
                "doctor: workdir is unsafe or could not be resolved",
                "twinning-mcp doctor --workdir <safe-directory>",
            ),
            EXIT_ENVIRONMENT_ERROR => (
                "doctor: twinning executable is missing or not running",
                "set TWINNING_BIN or pass --twinning-bin <path>",
            ),
            _ => (
                "doctor: twinning health or capability probe failed",
                "twinning doctor health --json",
            ),
        };
        eprintln!("error: {message}");
        eprintln!("next: {next}");
    }
    exit_code
}

fn selected_binary_exists(explicit: Option<&Path>, env_bin: Option<&Path>) -> bool {
    let selected_exists = [explicit, env_bin].into_iter().flatten().any(Path::is_file);
    if selected_exists {
        return true;
    }
    let sibling_exists = env::current_exe()
        .ok()
        .and_then(|executable| executable.parent().map(Path::to_path_buf))
        .is_some_and(|parent| parent.join("twinning").is_file());
    let path_exists = env::var_os("PATH")
        .map(|value| env::split_paths(&value).any(|entry| entry.join("twinning").is_file()))
        .unwrap_or(false);
    sibling_exists || path_exists
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
