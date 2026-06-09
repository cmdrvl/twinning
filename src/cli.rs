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
    long_about = "Prepare protocol-faithful interface twins for fast extractor iteration and \
migration proof. The Postgres pgwire twin is the v0 center; REST, MCP, and Snowflake \
wire twins are available behind feature flags. Every command refuses unsupported shapes \
explicitly instead of degrading silently.",
    after_help = AFTER_HELP,
    arg_required_else_help = true
)]
pub struct Cli {
    /// Emit machine-readable JSON instead of human text (global; valid on every subcommand)
    #[arg(long, global = true)]
    pub json: bool,

    /// Print the compiled operator manifest (operator.json) and exit 0 (global)
    #[arg(long, global = true)]
    pub describe: bool,

    #[command(subcommand)]
    pub command: Option<Command>,
}

/// Agent-facing discovery footer shown under `twinning --help`.
const AFTER_HELP: &str = "\
Discovery (read-only, safe to run blind):
  twinning --describe                  Print the operator.json manifest and exit
  twinning doctor capabilities --json  Machine-readable commands, output contracts, exit codes
  twinning doctor health --json        Read-only health diagnostics
  twinning doctor robot-docs           Concise agent-facing command notes (plain text)
  twinning doctor --robot-triage       Structured follow-up findings

Output contract:
  Add --json to any command for machine-readable output (data on stdout, diagnostics on stderr).
  Refusals carry a `next_command` field naming exactly what to run instead.

Exit codes:
  0  READY / clean bootstrap, run_once, or serve
  1  live finalization completed but embedded verify reported FAIL
  2  refusal, bootstrap failure, or CLI error

Example:
  twinning postgres --schema schema.sql --json";

#[derive(Debug, Subcommand, Clone)]
pub enum Command {
    #[command(
        about = "Prepare a Postgres interface twin",
        after_help = "Examples:\n  \
twinning postgres --schema schema.sql --json                       # validate schema, inspect bootstrap\n  \
twinning postgres --schema schema.sql --report out/r.json --snapshot out/r.twin --json\n  \
twinning postgres --schema schema.sql --run 'psql -h 127.0.0.1 ...' --json   # one live child command\n  \
twinning postgres --schema schema.sql --serve --json               # interactive twin until SIGINT\n  \
twinning postgres --restore out/r.twin --json                      # restore a prior snapshot\n\n\
Provide exactly one bootstrap source: --schema or --restore."
    )]
    Postgres(TwinArgs),
    #[command(about = "Declared but refused until the Postgres v0 center is real")]
    Mysql(TwinArgs),
    #[command(about = "Declared but refused until the Postgres v0 center is real")]
    Oracle(TwinArgs),
    #[cfg(feature = "rest")]
    #[command(
        about = "Prepare an OpenAPI-spec-driven REST interface twin with x-twinning fixtures"
    )]
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
    /// SQL DDL file defining tables, constraints, and indexes. Provide exactly one bootstrap source: --schema or --restore
    #[arg(long, value_name = "FILE")]
    pub schema: Option<PathBuf>,

    /// Compiled verify constraint artifact (verify.constraint.v1) for embedded twin-side validation
    #[arg(long, value_name = "FILE")]
    pub verify: Option<PathBuf>,

    /// Optional catalog-declared subset identity (twinning.catalog-declaration.v0)
    #[arg(long, value_name = "FILE")]
    pub declaration: Option<PathBuf>,

    /// Listen host for the pgwire shell
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Listen port [default: 5432 for postgres]
    #[arg(long)]
    pub port: Option<u16>,

    /// Run one child command against the live pgwire shell, then freeze final artifacts (mutually exclusive with --serve)
    #[arg(long, value_name = "COMMAND")]
    pub run: Option<String>,

    /// Run a standalone interactive pgwire shell until SIGINT/SIGTERM, then freeze final artifacts (mutually exclusive with --run)
    #[arg(long)]
    pub serve: bool,

    /// Write the twinning.v0 readiness report as JSON to this path
    #[arg(long, value_name = "FILE")]
    pub report: Option<PathBuf>,

    /// Write a deterministic twinning.snapshot.v0 bootstrap snapshot to this path
    #[arg(long, value_name = "FILE")]
    pub snapshot: Option<PathBuf>,

    /// Write a redacted live query-trace artifact (twinning.query-trace.v0); only meaningful with --run or --serve
    #[arg(long, value_name = "FILE")]
    pub query_trace: Option<PathBuf>,

    /// Restore a prior twinning.snapshot.v0 instead of loading --schema (provide exactly one of --schema or --restore)
    #[arg(long, value_name = "FILE")]
    pub restore: Option<PathBuf>,

    /// Write a twinning.seed-contract.v0 JSONL template for the schema-loaded catalog (requires --schema; not valid with --restore)
    #[arg(long, value_name = "FILE")]
    pub export_seed_contract: Option<PathBuf>,

    /// Import filled twinning.seed-data.v0 JSONL as committed state (requires --schema; cannot combine with --materialize-source-url)
    #[arg(long, value_name = "FILE")]
    pub seed: Option<PathBuf>,

    /// Capture declared source rows from a live Postgres URL into the final report/snapshot via psql COPY (requires --schema)
    #[arg(long, value_name = "URL")]
    pub materialize_source_url: Option<String>,
}

#[cfg(feature = "rest")]
#[derive(Debug, Args, Clone)]
pub struct RestArgs {
    /// OpenAPI 3.x YAML or JSON spec file [required]. Supports local refs; remote $ref URLs are fetched at startup
    #[arg(long, value_name = "FILE")]
    pub spec: Option<PathBuf>,

    /// REST listen host
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// REST listen port
    #[arg(long, default_value_t = 8080)]
    pub port: u16,

    /// Run one child command against the live REST server, then exit (mutually exclusive with --serve)
    #[arg(long, value_name = "COMMAND")]
    pub run: Option<String>,

    /// Run the interactive REST server until SIGINT/SIGTERM (default when neither --run nor --serve is given; mutually exclusive with --run)
    #[arg(long)]
    pub serve: bool,

    /// Write the REST session report as JSON (twinning.rest-report.v0)
    #[arg(long, value_name = "FILE")]
    pub report: Option<PathBuf>,

    /// Assert the REST session report against canary expectations
    #[arg(long, value_name = "FILE")]
    pub canary: Option<PathBuf>,

    /// Fail startup if any route cannot be classified
    #[arg(long)]
    pub strict: bool,

    /// Routing policy override [default: auto]
    #[arg(long, value_enum)]
    pub routing: Option<RoutingPolicy>,

    /// Explicit path prefix to strip before resource classification (e.g. /api, /__admin)
    #[arg(long, value_name = "PREFIX")]
    pub base_prefix: Option<String>,

    /// Override an OpenAPI server variable as NAME=VALUE before route classification (repeatable)
    #[arg(long = "server-variable", value_name = "NAME=VALUE")]
    pub server_variables: Vec<String>,

    /// REST auth shape mode override [default: shape]
    #[arg(long, value_enum)]
    pub auth_mode: Option<RestAuthMode>,

    /// Chaos-injection spec for fault testing (e.g. latency or error rates)
    #[arg(long, value_name = "SPEC")]
    pub chaos: Option<ChaosConfig>,
}

#[cfg(feature = "rest")]
#[derive(Debug, Args, Clone)]
pub struct PortArgs {
    /// First (source) OpenAPI spec for the dual-twin migration proof [required]
    #[arg(long, value_name = "FILE")]
    pub from_spec: PathBuf,

    /// Second (candidate) OpenAPI spec for the dual-twin migration proof [required]
    #[arg(long, value_name = "FILE")]
    pub to_spec: PathBuf,

    /// Client command run against both twins for interface-equivalence comparison [required]
    #[arg(long, value_name = "COMMAND")]
    pub client_cmd: String,

    /// Listen port for the source twin [default: auto-assigned]
    #[arg(long)]
    pub from_port: Option<u16>,

    /// Listen port for the candidate twin [default: auto-assigned]
    #[arg(long)]
    pub to_port: Option<u16>,

    /// Override an OpenAPI server variable on the source twin as NAME=VALUE (repeatable)
    #[arg(long = "from-server-variable", value_name = "NAME=VALUE")]
    pub from_server_variables: Vec<String>,

    /// Override an OpenAPI server variable on the candidate twin as NAME=VALUE (repeatable)
    #[arg(long = "to-server-variable", value_name = "NAME=VALUE")]
    pub to_server_variables: Vec<String>,

    /// Committed-state snapshot loaded into both twins
    #[arg(long, value_name = "FILE")]
    pub shared_snapshot: Option<PathBuf>,

    /// Committed-state snapshot for the source twin only
    #[arg(long, value_name = "FILE")]
    pub from_snapshot: Option<PathBuf>,

    /// Committed-state snapshot for the candidate twin only
    #[arg(long, value_name = "FILE")]
    pub to_snapshot: Option<PathBuf>,

    /// Write the dual-twin migration proof report as JSON
    #[arg(long, value_name = "FILE")]
    pub report: Option<PathBuf>,
}

#[cfg(feature = "mcp")]
#[derive(Debug, Args, Clone)]
pub struct McpArgs {
    /// Command that launches a live MCP server to introspect for the catalog (provide exactly one of --server or --manifest)
    #[arg(long, value_name = "COMMAND")]
    pub server: Option<String>,

    /// Static MCP manifest file describing the catalog (provide exactly one of --server or --manifest)
    #[arg(long, value_name = "FILE")]
    pub manifest: Option<PathBuf>,

    /// MCP HTTP listen host
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// MCP HTTP listen port
    #[arg(long, default_value_t = 9878)]
    pub port: u16,

    /// MCP auth shape mode
    #[arg(long, value_enum, default_value_t = RestAuthMode::Shape)]
    pub auth_mode: RestAuthMode,

    /// Use line-delimited JSON-RPC over stdin/stdout instead of HTTP (mutually exclusive with --run)
    #[arg(long)]
    pub stdio: bool,

    /// Run one child command against the live HTTP MCP server, then exit (mutually exclusive with --stdio)
    #[arg(long, value_name = "COMMAND")]
    pub run: Option<String>,

    /// Write the MCP session report as JSON (twinning.mcp-report.v0)
    #[arg(long, value_name = "FILE")]
    pub report: Option<PathBuf>,
}

#[cfg(feature = "snowflake")]
#[derive(Debug, Args, Clone)]
pub struct SnowflakeArgs {
    /// Snowflake DDL file seeding the catalog [required]
    #[arg(long, value_name = "FILE")]
    pub schema: Option<PathBuf>,

    /// Snowflake HTTP listen host
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// Snowflake HTTP listen port
    #[arg(long, default_value_t = 9876)]
    pub port: u16,

    /// Run one child command against the live Snowflake HTTP server, then exit (mutually exclusive with --serve)
    #[arg(long, value_name = "COMMAND")]
    pub run: Option<String>,

    /// Run the interactive Snowflake HTTP server until SIGINT/SIGTERM (mutually exclusive with --run)
    #[arg(long)]
    pub serve: bool,

    /// Write the Snowflake session report as JSON (twinning.snowflake-report.v0)
    #[arg(long, value_name = "FILE")]
    pub report: Option<PathBuf>,

    /// Capture declared source rows from a live Snowflake URL into the final report
    #[arg(long, value_name = "URL")]
    pub materialize_source_url: Option<String>,

    /// Maximum rows captured per table during materialization
    #[arg(long, default_value_t = 100_000)]
    pub max_rows_per_table: usize,
}

#[derive(Debug, Args, Clone)]
pub struct DoctorArgs {
    /// Emit structured triage findings as JSON without reading or writing artifacts
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

    /// Left (legacy) committed-state snapshot to compare
    #[arg(long, value_name = "FILE")]
    pub left: Option<PathBuf>,

    /// Right (candidate) committed-state snapshot to compare
    #[arg(long, value_name = "FILE")]
    pub right: Option<PathBuf>,

    /// Query-case fixture (JSON/JSONL) defining the replay set for the proof
    #[arg(long, value_name = "FILE")]
    pub queries: Option<PathBuf>,

    /// Write the twin-pair proof report as JSON (twinning.twin-pair-proof.v0)
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
    /// Twin-pair proof orchestration manifest (twinning.twin-pair-orchestration-manifest.v0) [required]
    #[arg(long, value_name = "FILE")]
    pub manifest: PathBuf,

    /// Write the twin-pair proof report as JSON
    #[arg(long, value_name = "FILE")]
    pub report: Option<PathBuf>,

    /// Directory to write the proof bundle into
    #[arg(long, value_name = "DIR")]
    pub bundle_dir: Option<PathBuf>,
}
