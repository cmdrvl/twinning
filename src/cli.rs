use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum Engine {
    Postgres,
    Mysql,
    Oracle,
}

impl Engine {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Mysql => "mysql",
            Self::Oracle => "oracle",
        }
    }

    pub fn default_port(self) -> u16 {
        match self {
            Self::Postgres => 5432,
            Self::Mysql => 3306,
            Self::Oracle => 1521,
        }
    }
}

impl std::fmt::Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

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
    #[command(about = "Inspect the CLI's read-only health and agent-facing capabilities")]
    Doctor(DoctorArgs),
}

impl Command {
    pub fn engine(&self) -> Option<Engine> {
        match self {
            Self::Postgres(_) => Some(Engine::Postgres),
            Self::Mysql(_) => Some(Engine::Mysql),
            Self::Oracle(_) => Some(Engine::Oracle),
            Self::Doctor(_) => None,
        }
    }

    pub fn twin_args(&self) -> Option<&TwinArgs> {
        match self {
            Self::Postgres(args) | Self::Mysql(args) | Self::Oracle(args) => Some(args),
            Self::Doctor(_) => None,
        }
    }
}

#[derive(Debug, Args, Clone)]
pub struct TwinArgs {
    #[arg(long, value_name = "FILE")]
    pub schema: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    pub verify: Option<PathBuf>,

    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    #[arg(long)]
    pub port: Option<u16>,

    #[arg(long, value_name = "COMMAND")]
    pub run: Option<String>,

    #[arg(long, value_name = "FILE")]
    pub report: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    pub snapshot: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    pub restore: Option<PathBuf>,
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
