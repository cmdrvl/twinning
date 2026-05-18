use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
