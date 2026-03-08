use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

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
    pub verify_artifact: Option<VerifyArtifactReport>,
    pub catalog: CatalogReport,
    pub storage: StorageReport,
    pub tables: BTreeMap<String, TableReport>,
    pub constraints: ConstraintCounters,
    pub verify: VerifyExecutionReport,
    pub snapshot: SnapshotReport,
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
pub struct VerifyExecutionReport {
    pub loaded: usize,
    pub evaluated: usize,
    pub pass: usize,
    pub fail: usize,
    pub violations: Vec<RuleViolation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleViolation {
    pub rule_id: String,
    pub count: u64,
    pub sample: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotReport {
    pub restored_from: Option<String>,
    pub written_to: Option<String>,
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

        let loaded_rules = seed
            .verify_artifact
            .as_ref()
            .map_or(0, |artifact| artifact.loaded);

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
            verify: VerifyExecutionReport {
                loaded: loaded_rules,
                evaluated: 0,
                pass: 0,
                fail: 0,
                violations: Vec::new(),
            },
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
