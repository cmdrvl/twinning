use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::catalog::Catalog;

pub const REPORT_VERSION: &str = "twinning.v0";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TwinReport {
    pub version: String,
    pub outcome: String,
    pub mode: String,
    pub engine: String,
    pub host: String,
    pub port: u16,
    pub wire_protocol: String,
    pub schema: SchemaReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verify_artifact: Option<VerifyArtifactReport>,
    pub catalog: CatalogReport,
    pub storage: StorageReport,
    pub tables: BTreeMap<String, TableReport>,
    pub constraints: ConstraintCounters,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verify: Option<Value>,
    pub snapshot: SnapshotReport,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
    pub next_step: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaReport {
    pub source: String,
    pub hash: String,
    pub table_count: usize,
    pub column_count: usize,
    pub index_count: usize,
    pub constraint_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerifyArtifactReport {
    pub source: String,
    pub hash: String,
    pub loaded: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogReport {
    pub dialect: String,
    pub table_count: usize,
    pub column_count: usize,
    pub index_count: usize,
    pub constraint_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageReport {
    pub tournament_mode: String,
    pub replay_mode: String,
    pub hot_working_set: String,
    pub cold_state: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableReport {
    pub rows: u64,
    pub expected: Option<u64>,
    pub coverage: Option<f64>,
    pub columns: usize,
    pub indexes: usize,
    pub constraints: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConstraintCounters {
    pub not_null_violations: u64,
    pub fk_violations: u64,
    pub check_violations: u64,
    pub unique_violations: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotReport {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restored_from: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub written_to: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_hash: Option<String>,
}

pub struct TwinReportSeed<'a> {
    pub engine: &'a str,
    pub host: &'a str,
    pub port: u16,
    pub schema: SchemaReport,
    pub verify_artifact: Option<VerifyArtifactReport>,
    pub catalog: &'a Catalog,
    pub snapshot: SnapshotReport,
    pub warnings: Vec<String>,
}

impl TwinReport {
    pub fn from_seed(seed: TwinReportSeed<'_>) -> Self {
        let tables = seed
            .catalog
            .tables
            .iter()
            .map(|table| {
                (
                    table.name.clone(),
                    TableReport {
                        rows: 0,
                        expected: None,
                        coverage: None,
                        columns: table.columns.len(),
                        indexes: table.indexes.len(),
                        constraints: table.constraint_count(),
                    },
                )
            })
            .collect();

        Self {
            version: REPORT_VERSION.to_owned(),
            outcome: "READY".to_owned(),
            mode: "bootstrap".to_owned(),
            engine: seed.engine.to_owned(),
            host: seed.host.to_owned(),
            port: seed.port,
            wire_protocol: "planned.pgwire".to_owned(),
            schema: seed.schema,
            verify_artifact: seed.verify_artifact,
            catalog: CatalogReport {
                dialect: seed.catalog.dialect.clone(),
                table_count: seed.catalog.table_count,
                column_count: seed.catalog.column_count,
                index_count: seed.catalog.index_count,
                constraint_count: seed.catalog.constraint_count,
            },
            storage: StorageReport {
                tournament_mode: "bounded-memory hot working set with per-twin overlay".to_owned(),
                replay_mode:
                    "disk-backed, snapshot-backed, or delegated real-database backend".to_owned(),
                hot_working_set: "memory".to_owned(),
                cold_state: "shared snapshot or pluggable backing store".to_owned(),
            },
            tables,
            constraints: ConstraintCounters {
                not_null_violations: 0,
                fk_violations: 0,
                check_violations: 0,
                unique_violations: 0,
            },
            verify: None,
            snapshot: seed.snapshot,
            warnings: seed.warnings,
            next_step: "Live pgwire execution is not implemented yet. Use this build to validate schema assets, emit deterministic bootstrap artifacts, and stage the runtime boundary cleanly.".to_owned(),
        }
    }

    pub fn render_json(&self) -> Result<String, serde_json::Error> {
        let mut rendered = serde_json::to_string_pretty(self)?;
        rendered.push('\n');
        Ok(rendered)
    }

    pub fn render_human(&self) -> String {
        let mut lines = vec![
            format!("twinning {} bootstrap ready", self.engine),
            format!("endpoint: {}:{}", self.host, self.port),
            format!(
                "schema: {} ({} tables, {} columns, {} indexes, hash {})",
                self.schema.source,
                self.schema.table_count,
                self.schema.column_count,
                self.schema.index_count,
                self.schema.hash
            ),
            format!(
                "storage: tournament={} | replay={}",
                self.storage.tournament_mode, self.storage.replay_mode
            ),
        ];

        if let Some(verify_artifact) = &self.verify_artifact {
            lines.push(format!(
                "verify: {} ({} loaded, hash {})",
                verify_artifact.source, verify_artifact.loaded, verify_artifact.hash
            ));
        }

        if let Some(restored) = &self.snapshot.restored_from {
            lines.push(format!("restored: {restored}"));
        }
        if let Some(written) = &self.snapshot.written_to {
            lines.push(format!(
                "snapshot: {} ({})",
                written,
                self.snapshot
                    .snapshot_hash
                    .as_deref()
                    .unwrap_or("hash unavailable")
            ));
        }
        if !self.warnings.is_empty() {
            lines.push(format!("warnings: {}", self.warnings.join(" | ")));
        }
        lines.push(format!("next: {}", self.next_step));
        lines.push(String::new());
        lines.join("\n")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        CatalogReport, ConstraintCounters, SchemaReport, SnapshotReport, StorageReport, TwinReport,
    };

    #[test]
    fn render_json_omits_absent_optional_sections() {
        let report = TwinReport {
            version: "twinning.v0".to_owned(),
            outcome: "READY".to_owned(),
            mode: "bootstrap".to_owned(),
            engine: "postgres".to_owned(),
            host: "127.0.0.1".to_owned(),
            port: 5432,
            wire_protocol: "planned.pgwire".to_owned(),
            schema: SchemaReport {
                source: "schema.sql".to_owned(),
                hash: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_owned(),
                table_count: 0,
                column_count: 0,
                index_count: 0,
                constraint_count: 0,
            },
            verify_artifact: None,
            catalog: CatalogReport {
                dialect: "postgres".to_owned(),
                table_count: 0,
                column_count: 0,
                index_count: 0,
                constraint_count: 0,
            },
            storage: StorageReport {
                tournament_mode: "overlay".to_owned(),
                replay_mode: "delegated".to_owned(),
                hot_working_set: "memory".to_owned(),
                cold_state: "snapshot".to_owned(),
            },
            tables: BTreeMap::new(),
            constraints: ConstraintCounters {
                not_null_violations: 0,
                fk_violations: 0,
                check_violations: 0,
                unique_violations: 0,
            },
            verify: None,
            snapshot: SnapshotReport {
                restored_from: None,
                written_to: None,
                snapshot_hash: None,
            },
            warnings: Vec::new(),
            next_step: "next".to_owned(),
        };

        let rendered = report.render_json().expect("render json");
        let json: serde_json::Value = serde_json::from_str(&rendered).expect("parse json");

        assert!(json.get("verify_artifact").is_none());
        assert!(json.get("verify").is_none());
        assert!(json.get("warnings").is_none());
        assert_eq!(json["snapshot"], serde_json::json!({}));
    }
}
