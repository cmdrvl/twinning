use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

pub use twinning_kernel::Engine;
#[cfg(any(feature = "rest", feature = "mcp"))]
use twinning_rest::auth::RestAuthMode;
#[cfg(feature = "rest")]
use twinning_rest::{config::ChaosConfig, policy::RoutingPolicy};

#[derive(Debug, Parser, Clone)]
#[command(
    name = "twinning",
    version,
    about = "Prepare a Postgres-first interface twin from schema assets",
    arg_required_else_help = true
)]
pub struct Cli {
    #[arg(long, global = true)]
    pub json: bool,

    #[arg(long, global = true)]
    pub describe: bool,

    #[command(subcommand)]
    pub command: Option<Command>,
}

#[derive(Debug, Subcommand, Clone)]
pub enum Command {
    #[command(about = "Prepare a Postgres interface twin")]
    Postgres(TwinArgs),
    #[command(about = "Declared but refused until the Postgres v0 center is real")]
    Mysql(TwinArgs),
    #[command(about = "Declared but refused until the Postgres v0 center is real")]
    Oracle(TwinArgs),
    #[cfg(feature = "rest")]
    #[command(about = "Prepare an OpenAPI-spec-driven REST interface twin")]
    Rest(RestArgs),
    #[cfg(feature = "rest")]
    #[command(about = "Run dual REST twins for client migration proof")]
    Port(PortArgs),
    #[cfg(feature = "mcp")]
    #[command(about = "Prepare a Model Context Protocol JSON-RPC interface twin")]
    Mcp(McpArgs),
    #[cfg(feature = "snowflake")]
    #[command(about = "Prepare a Snowflake HTTP wire protocol interface twin")]
    Snowflake(SnowflakeArgs),
    #[command(about = "Inspect the CLI's read-only health and agent-facing capabilities")]
    Doctor(DoctorArgs),
    #[command(about = "Run proof-oriented twin comparison workflows")]
    Proof(ProofArgs),
}

impl Command {
    pub fn engine(&self) -> Option<Engine> {
        match self {
            Self::Postgres(_) => Some(Engine::Postgres),
            Self::Mysql(_) => Some(Engine::Mysql),
            Self::Oracle(_) => Some(Engine::Oracle),
            #[cfg(feature = "rest")]
            Self::Rest(_) | Self::Port(_) => None,
            #[cfg(feature = "mcp")]
            Self::Mcp(_) => None,
            #[cfg(feature = "snowflake")]
            Self::Snowflake(_) => None,
            Self::Doctor(_) | Self::Proof(_) => None,
        }
    }

    pub fn twin_args(&self) -> Option<&TwinArgs> {
        match self {
            Self::Postgres(args) | Self::Mysql(args) | Self::Oracle(args) => Some(args),
            #[cfg(feature = "rest")]
            Self::Rest(_) | Self::Port(_) => None,
            #[cfg(feature = "mcp")]
            Self::Mcp(_) => None,
            #[cfg(feature = "snowflake")]
            Self::Snowflake(_) => None,
            Self::Doctor(_) | Self::Proof(_) => None,
        }
    }

    #[cfg(feature = "rest")]
    pub fn rest_args(&self) -> Option<&RestArgs> {
        match self {
            Self::Rest(args) => Some(args),
            Self::Postgres(_)
            | Self::Mysql(_)
            | Self::Oracle(_)
            | Self::Port(_)
            | Self::Doctor(_)
            | Self::Proof(_) => None,
            #[cfg(feature = "mcp")]
            Self::Mcp(_) => None,
            #[cfg(feature = "snowflake")]
            Self::Snowflake(_) => None,
        }
    }

    #[cfg(feature = "mcp")]
    pub fn mcp_args(&self) -> Option<&McpArgs> {
        match self {
            Self::Mcp(args) => Some(args),
            Self::Postgres(_)
            | Self::Mysql(_)
            | Self::Oracle(_)
            | Self::Doctor(_)
            | Self::Proof(_) => None,
            #[cfg(feature = "rest")]
            Self::Rest(_) | Self::Port(_) => None,
            #[cfg(feature = "snowflake")]
            Self::Snowflake(_) => None,
        }
    }

    #[cfg(feature = "snowflake")]
    pub fn snowflake_args(&self) -> Option<&SnowflakeArgs> {
        match self {
            Self::Snowflake(args) => Some(args),
            Self::Postgres(_)
            | Self::Mysql(_)
            | Self::Oracle(_)
            | Self::Doctor(_)
            | Self::Proof(_) => None,
            #[cfg(feature = "rest")]
            Self::Rest(_) | Self::Port(_) => None,
            #[cfg(feature = "mcp")]
            Self::Mcp(_) => None,
        }
    }
}

#[derive(Debug, Args, Clone)]
pub struct TwinArgs {
    #[arg(long, value_name = "FILE")]
    pub schema: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    pub verify: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    pub declaration: Option<PathBuf>,

    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    #[arg(long)]
    pub port: Option<u16>,

    #[arg(long, value_name = "COMMAND")]
    pub run: Option<String>,

    #[arg(long)]
    pub serve: bool,

    #[arg(long, value_name = "FILE")]
    pub report: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    pub snapshot: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    pub query_trace: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    pub restore: Option<PathBuf>,

    #[arg(long, value_name = "URL")]
    pub materialize_source_url: Option<String>,
}

#[cfg(feature = "rest")]
#[derive(Debug, Args, Clone)]
pub struct RestArgs {
    #[arg(long, value_name = "FILE")]
    pub spec: Option<PathBuf>,

    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    #[arg(long, default_value_t = 8080)]
    pub port: u16,

    #[arg(long, value_name = "COMMAND")]
    pub run: Option<String>,

    #[arg(long)]
    pub serve: bool,

    #[arg(long, value_name = "FILE")]
    pub report: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    pub canary: Option<PathBuf>,

    #[arg(long)]
    pub strict: bool,

    #[arg(long, value_enum)]
    pub routing: Option<RoutingPolicy>,

    #[arg(long, value_name = "PREFIX")]
    pub base_prefix: Option<String>,

    #[arg(long, value_enum)]
    pub auth_mode: Option<RestAuthMode>,

    #[arg(long, value_name = "SPEC")]
    pub chaos: Option<ChaosConfig>,
}

#[cfg(feature = "rest")]
#[derive(Debug, Args, Clone)]
pub struct PortArgs {
    #[arg(long, value_name = "FILE")]
    pub from_spec: PathBuf,

    #[arg(long, value_name = "FILE")]
    pub to_spec: PathBuf,

    #[arg(long, value_name = "COMMAND")]
    pub client_cmd: String,

    #[arg(long)]
    pub from_port: Option<u16>,

    #[arg(long)]
    pub to_port: Option<u16>,

    #[arg(long, value_name = "FILE")]
    pub shared_snapshot: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    pub from_snapshot: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    pub to_snapshot: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    pub report: Option<PathBuf>,
}

#[cfg(feature = "mcp")]
#[derive(Debug, Args, Clone)]
pub struct McpArgs {
    #[arg(long, value_name = "COMMAND")]
    pub server: Option<String>,

    #[arg(long, value_name = "FILE")]
    pub manifest: Option<PathBuf>,

    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    #[arg(long, default_value_t = 9878)]
    pub port: u16,

    #[arg(long, value_enum, default_value_t = RestAuthMode::Shape)]
    pub auth_mode: RestAuthMode,

    #[arg(long)]
    pub stdio: bool,

    #[arg(long, value_name = "COMMAND")]
    pub run: Option<String>,

    #[arg(long, value_name = "FILE")]
    pub report: Option<PathBuf>,
}

#[cfg(feature = "snowflake")]
#[derive(Debug, Args, Clone)]
pub struct SnowflakeArgs {
    #[arg(long, value_name = "FILE")]
    pub schema: Option<PathBuf>,

    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    #[arg(long, default_value_t = 9876)]
    pub port: u16,

    #[arg(long, value_name = "COMMAND")]
    pub run: Option<String>,

    #[arg(long)]
    pub serve: bool,

    #[arg(long, value_name = "FILE")]
    pub report: Option<PathBuf>,

    #[arg(long, value_name = "URL")]
    pub materialize_source_url: Option<String>,

    #[arg(long, default_value_t = 100_000)]
    pub max_rows_per_table: usize,
}

#[derive(Debug, Args, Clone)]
pub struct DoctorArgs {
    #[arg(long)]
    pub robot_triage: bool,

    #[command(subcommand)]
    pub command: Option<DoctorCommand>,
}

#[derive(Debug, Subcommand, Clone, Copy, PartialEq, Eq)]
pub enum DoctorCommand {
    #[command(about = "Emit read-only health checks")]
    Health,
    #[command(about = "Emit machine-readable capability metadata")]
    Capabilities,
    #[command(name = "robot-docs", about = "Print concise agent-facing usage notes")]
    RobotDocs,
}

#[derive(Debug, Args, Clone)]
pub struct ProofArgs {
    #[command(subcommand)]
    pub command: ProofCommand,
}

#[derive(Debug, Subcommand, Clone)]
pub enum ProofCommand {
    #[command(name = "twin-pair", about = "Compare two Postgres twin snapshots")]
    TwinPair(TwinPairProofArgs),
}

#[derive(Debug, Args, Clone)]
pub struct TwinPairProofArgs {
    #[command(subcommand)]
    pub command: Option<TwinPairProofCommand>,

    #[arg(long, value_name = "FILE")]
    pub left: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    pub right: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    pub queries: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    pub report: Option<PathBuf>,
}

#[derive(Debug, Subcommand, Clone)]
pub enum TwinPairProofCommand {
    #[command(
        name = "orchestrate",
        about = "Run a manifest-first twin-pair proof orchestration"
    )]
    Orchestrate(TwinPairOrchestrateArgs),
}

#[derive(Debug, Args, Clone)]
pub struct TwinPairOrchestrateArgs {
    #[arg(long, value_name = "FILE")]
    pub manifest: PathBuf,

    #[arg(long, value_name = "FILE")]
    pub report: Option<PathBuf>,

    #[arg(long, value_name = "DIR")]
    pub bundle_dir: Option<PathBuf>,
}
