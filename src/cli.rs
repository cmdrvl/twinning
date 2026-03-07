use std::path::PathBuf;

use clap::{Parser, ValueEnum};
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
    about = "Prepare a Postgres-first interface twin from schema assets"
)]
pub struct Cli {
    #[arg(value_enum)]
    pub engine: Engine,

    #[arg(long, value_name = "FILE")]
    pub schema: Option<PathBuf>,

    #[arg(long, value_name = "FILE")]
    pub rules: Option<PathBuf>,

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

    #[arg(long)]
    pub json: bool,

    #[arg(long)]
    pub describe: bool,
}
