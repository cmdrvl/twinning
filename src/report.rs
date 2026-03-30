use std::{collections::BTreeMap, path::Path};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use verify_core::constraint::{Check, ConstraintSet, Portability};
use verify_core::{
    REPORT_VERSION as VERIFY_REPORT_VERSION, TOOL_NAME as VERIFY_TOOL_NAME, report::VerifyReport,
};

use crate::catalog::Catalog;

pub const REPORT_VERSION: &str = "twinning.v0";
const VERIFY_CONSTRAINT_VERSION: &str = "verify.constraint.v1";

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub run: Option<RunReport>,
    #[serde(skip_serializing_if = "optional_ratio_map_is_absent")]
    pub null_rates: Option<RatioMap>,
    #[serde(skip_serializing_if = "optional_ratio_map_is_absent")]
    pub fk_coverage: Option<RatioMap>,
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

#[derive(Debug, Clone, PartialEq)]
pub struct LiveVerifyArtifact {
    pub report: VerifyArtifactReport,
    pub constraint_set_id: String,
    pub batch_only_rule: Option<BatchOnlyRule>,
    pub constraint_set: ConstraintSet,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchOnlyRule {
    pub rule_id: String,
    pub op: String,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunReport {
    pub command: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    pub timed_out: bool,
}

pub type RatioMap = BTreeMap<String, f64>;

pub struct TwinReportSeed<'a> {
    pub engine: &'a str,
    pub host: &'a str,
    pub port: u16,
    pub schema: SchemaReport,
    pub verify_artifact: Option<VerifyArtifactReport>,
    pub verify: Option<Value>,
    pub catalog: &'a Catalog,
    pub snapshot: SnapshotReport,
    pub run: Option<RunReport>,
    pub null_rates: Option<RatioMap>,
    pub fk_coverage: Option<RatioMap>,
    pub warnings: Vec<String>,
}

impl TwinReport {
    pub fn from_seed(seed: TwinReportSeed<'_>) -> Self {
        let mode = if seed.run.is_some() {
            "run_once"
        } else {
            "bootstrap"
        };
        let outcome = report_outcome(seed.run.as_ref(), seed.verify.as_ref());
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
            outcome: outcome.to_owned(),
            mode: mode.to_owned(),
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
                replay_mode: "disk-backed, snapshot-backed, or delegated real-database backend"
                    .to_owned(),
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
            verify: seed.verify,
            snapshot: seed.snapshot,
            run: seed.run,
            null_rates: seed.null_rates,
            fk_coverage: seed.fk_coverage,
            warnings: seed.warnings,
            next_step: next_step(mode),
        }
    }

    pub fn attach_verify_report(&mut self, verify: Value) -> Result<(), String> {
        let parsed: VerifyReport = serde_json::from_value(verify.clone()).map_err(|error| {
            format!("verify payload must be schema-correct `verify.report.v1`: {error}")
        })?;

        if parsed.tool != VERIFY_TOOL_NAME {
            return Err(format!(
                "verify payload must declare tool `{VERIFY_TOOL_NAME}`, found `{}`",
                parsed.tool
            ));
        }

        let version = verify
            .get("version")
            .and_then(Value::as_str)
            .ok_or_else(|| String::from("verify payload must include string field `version`"))?;
        if version != VERIFY_REPORT_VERSION {
            return Err(format!(
                "verify payload must declare version `{VERIFY_REPORT_VERSION}`, found `{version}`"
            ));
        }

        self.verify = Some(verify);
        self.outcome = report_outcome(self.run.as_ref(), self.verify.as_ref()).to_owned();
        Ok(())
    }

    pub fn attach_null_rates(&mut self, null_rates: RatioMap) {
        self.null_rates = if null_rates.is_empty() {
            None
        } else {
            Some(null_rates)
        };
    }

    pub fn attach_fk_coverage(&mut self, fk_coverage: RatioMap) {
        self.fk_coverage = if fk_coverage.is_empty() {
            None
        } else {
            Some(fk_coverage)
        };
    }

    pub fn render_json(&self) -> Result<String, serde_json::Error> {
        let mut rendered = serde_json::to_string_pretty(self)?;
        rendered.push('\n');
        Ok(rendered)
    }

    pub fn render_human(&self) -> String {
        let mut lines = vec![
            format!(
                "twinning {} {} {}",
                self.engine,
                self.mode,
                self.outcome.to_ascii_lowercase()
            ),
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
        if let Some(run) = &self.run {
            lines.push(format!(
                "run: {} | exit_code={} | signal={} | timed_out={}",
                run.command,
                run.exit_code
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| String::from("<none>")),
                run.signal
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| String::from("<none>")),
                run.timed_out
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

fn optional_ratio_map_is_absent(value: &Option<RatioMap>) -> bool {
    match value {
        None => true,
        Some(value) => value.is_empty(),
    }
}

fn run_failed(run: &RunReport) -> bool {
    run.timed_out || run.signal.is_some() || run.exit_code != Some(0)
}

fn report_outcome(run: Option<&RunReport>, verify: Option<&Value>) -> &'static str {
    if run.is_some_and(run_failed) {
        return "FAIL";
    }

    match verify_outcome(verify) {
        Some("FAIL") => "FAIL",
        Some("PASS") => "PASS",
        _ => "READY",
    }
}

fn verify_outcome(verify: Option<&Value>) -> Option<&str> {
    verify
        .and_then(|verify| verify.get("outcome"))
        .and_then(Value::as_str)
}

fn next_step(mode: &str) -> String {
    if mode == "run_once" {
        return String::from(
            "Inspect the run metadata, fix the child failure or drift, and rerun the candidate against the twin.",
        );
    }

    String::from(
        "Bootstrap mode validated the schema assets and deterministic artifact path. Use --run to exercise the declared live Postgres subset, or stay in bootstrap mode while broader protocol and SQL coverage lands.",
    )
}

impl LiveVerifyArtifact {
    pub fn load(path: &Path) -> Result<Self, String> {
        let bytes = std::fs::read(path)
            .map_err(|error| format!("failed to read `{}`: {error}", path.display()))?;
        Self::parse(path.display().to_string(), &bytes)
    }

    pub fn parse(source: impl Into<String>, bytes: &[u8]) -> Result<Self, String> {
        let source = source.into();
        let constraint_set: ConstraintSet = serde_json::from_slice(bytes)
            .map_err(|error| format!("failed to parse `{source}`: {error}"))?;

        let version = constraint_set.version.as_str();
        if version != VERIFY_CONSTRAINT_VERSION {
            return Err(format!(
                "`{source}` must declare version `{VERIFY_CONSTRAINT_VERSION}`, found `{version}`"
            ));
        }

        let batch_only_rule = constraint_set
            .rules
            .iter()
            .into_iter()
            .find(|rule| {
                matches!(rule.portability, Portability::BatchOnly)
                    || matches!(rule.check, Check::QueryZeroRows { .. })
            })
            .map(|rule| BatchOnlyRule {
                rule_id: rule.id.clone(),
                op: rule.check.op().to_owned(),
            });

        Ok(Self {
            report: VerifyArtifactReport {
                source,
                hash: sha256_prefixed(bytes),
                loaded: constraint_set.rules.len(),
            },
            constraint_set_id: constraint_set.constraint_set_id.clone(),
            batch_only_rule,
            constraint_set,
        })
    }
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("sha256:{:x}", digest.finalize())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use crate::catalog::parse_postgres_schema;

    use super::{
        CatalogReport, ConstraintCounters, LiveVerifyArtifact, RatioMap, RunReport, SchemaReport,
        SnapshotReport, StorageReport, TwinReport, TwinReportSeed,
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
            run: None,
            null_rates: None,
            fk_coverage: None,
            warnings: Vec::new(),
            next_step: "next".to_owned(),
        };

        let rendered = report.render_json().expect("render json");
        let json: serde_json::Value = serde_json::from_str(&rendered).expect("parse json");

        assert!(json.get("verify_artifact").is_none());
        assert!(json.get("verify").is_none());
        assert!(json.get("run").is_none());
        assert!(json.get("null_rates").is_none());
        assert!(json.get("fk_coverage").is_none());
        assert!(json.get("warnings").is_none());
        assert_eq!(json["snapshot"], serde_json::json!({}));
    }

    #[test]
    fn run_report_serializes_only_present_optional_fields() {
        let report = TwinReport {
            version: "twinning.v0".to_owned(),
            outcome: "FAIL".to_owned(),
            mode: "run_once".to_owned(),
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
            run: Some(RunReport {
                command: String::from("python extract.py"),
                exit_code: None,
                signal: Some(15),
                timed_out: true,
            }),
            null_rates: None,
            fk_coverage: None,
            warnings: Vec::new(),
            next_step: "next".to_owned(),
        };

        let rendered = report.render_json().expect("render json");
        let json: serde_json::Value = serde_json::from_str(&rendered).expect("parse json");

        assert_eq!(json["run"]["command"], "python extract.py");
        assert!(json["run"].get("exit_code").is_none());
        assert_eq!(json["run"]["signal"], 15);
        assert_eq!(json["run"]["timed_out"], true);
    }

    #[test]
    fn run_once_human_output_uses_run_mode_failure_header_and_guidance() {
        let catalog = parse_postgres_schema(
            r#"
            CREATE TABLE public.deals (
                deal_id TEXT PRIMARY KEY,
                deal_name TEXT NOT NULL
            );
            "#,
        )
        .expect("parse schema");

        let report = TwinReport::from_seed(TwinReportSeed {
            engine: "postgres",
            host: "127.0.0.1",
            port: 5432,
            schema: SchemaReport {
                source: "schema.sql".to_owned(),
                hash: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_owned(),
                table_count: catalog.table_count,
                column_count: catalog.column_count,
                index_count: catalog.index_count,
                constraint_count: catalog.constraint_count,
            },
            verify_artifact: None,
            verify: None,
            catalog: &catalog,
            snapshot: SnapshotReport {
                restored_from: None,
                written_to: None,
                snapshot_hash: None,
            },
            run: Some(RunReport {
                command: String::from("python extract.py"),
                exit_code: Some(7),
                signal: None,
                timed_out: false,
            }),
            null_rates: None,
            fk_coverage: None,
            warnings: Vec::new(),
        });

        assert_eq!(report.mode, "run_once");
        assert_eq!(report.outcome, "FAIL");
        assert_eq!(
            report.next_step,
            "Inspect the run metadata, fix the child failure or drift, and rerun the candidate against the twin."
        );

        let rendered = report.render_human();
        assert!(rendered.starts_with("twinning postgres run_once fail\n"));
        assert!(
            rendered
                .contains("run: python extract.py | exit_code=7 | signal=<none> | timed_out=false")
        );
        assert!(rendered.contains(&format!("next: {}\n", report.next_step)));
    }

    #[test]
    fn verify_payload_and_twin_native_metrics_render_as_separate_sections() {
        let mut report = TwinReport {
            version: "twinning.v0".to_owned(),
            outcome: "FAIL".to_owned(),
            mode: "run_once".to_owned(),
            engine: "postgres".to_owned(),
            host: "127.0.0.1".to_owned(),
            port: 5432,
            wire_protocol: "planned.pgwire".to_owned(),
            schema: SchemaReport {
                source: "schema.sql".to_owned(),
                hash: "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_owned(),
                table_count: 2,
                column_count: 5,
                index_count: 2,
                constraint_count: 2,
            },
            verify_artifact: None,
            catalog: CatalogReport {
                dialect: "postgres".to_owned(),
                table_count: 2,
                column_count: 5,
                index_count: 2,
                constraint_count: 2,
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
            run: None,
            null_rates: None,
            fk_coverage: None,
            warnings: Vec::new(),
            next_step: "next".to_owned(),
        };

        report
            .attach_verify_report(json!({
                "tool": "verify",
                "version": "verify.report.v1",
                "execution_mode": "embedded",
                "outcome": "FAIL",
                "constraint_set_id": "deals-portable",
                "constraint_hash": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "bindings": {},
                "summary": {
                    "total_rules": 2,
                    "passed_rules": 1,
                    "failed_rules": 1,
                    "by_severity": {
                        "error": 1,
                        "warn": 0
                    }
                },
                "policy_signals": {
                    "severity_band": "ERROR_PRESENT"
                },
                "results": [],
                "refusal": null
            }))
            .expect("attach verify report");
        report.attach_null_rates(RatioMap::from([(
            String::from("public.deals.deal_name"),
            0.25,
        )]));
        report.attach_fk_coverage(RatioMap::from([(
            String::from("public.deals.tenant_id -> public.tenants.tenant_id"),
            1.0,
        )]));

        let rendered = report.render_json().expect("render json");
        let json: serde_json::Value = serde_json::from_str(&rendered).expect("parse json");

        assert_eq!(json["outcome"], "FAIL");
        assert_eq!(json["verify"]["version"], "verify.report.v1");
        assert_eq!(json["verify"]["summary"]["failed_rules"], 1);
        assert_eq!(json["null_rates"]["public.deals.deal_name"], 0.25);
        assert_eq!(
            json["fk_coverage"]["public.deals.tenant_id -> public.tenants.tenant_id"],
            1.0
        );
    }

    #[test]
    fn verify_attachment_refuses_non_verify_report_payloads() {
        let mut report = TwinReport {
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
            run: None,
            null_rates: None,
            fk_coverage: None,
            warnings: Vec::new(),
            next_step: "next".to_owned(),
        };

        let error = report
            .attach_verify_report(json!({
                "version": "verify.constraint.v1",
                "outcome": "READY"
            }))
            .expect_err("non-report payload should be rejected");

        assert!(
            error.contains("verify.report.v1"),
            "unexpected error: {error}"
        );
        assert!(report.verify.is_none());
    }

    #[test]
    fn verify_attachment_refuses_partial_verify_report_payloads() {
        let mut report = TwinReport {
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
            run: None,
            null_rates: None,
            fk_coverage: None,
            warnings: Vec::new(),
            next_step: "next".to_owned(),
        };

        let error = report
            .attach_verify_report(json!({
                "tool": "verify",
                "version": "verify.report.v1",
                "outcome": "FAIL"
            }))
            .expect_err("partial verify payload should be rejected");

        assert!(
            error.contains("schema-correct `verify.report.v1`"),
            "unexpected error: {error}"
        );
        assert!(report.verify.is_none());
    }

    #[test]
    fn live_verify_artifact_parse_preserves_identity_and_loaded_count() {
        let artifact = LiveVerifyArtifact::parse(
            "constraints.verify.json",
            br#"{
              "version": "verify.constraint.v1",
              "constraint_set_id": "portfolio.loan_tape.v1",
              "bindings": [{ "name": "input", "kind": "relation" }],
              "rules": [
                {
                  "id": "LOAN_ID_PRESENT",
                  "severity": "error",
                  "portability": "portable",
                  "check": { "op": "not_null", "binding": "input", "columns": ["loan_id"] }
                },
                {
                  "id": "BALANCE_POSITIVE",
                  "severity": "error",
                  "portability": "portable",
                  "check": {
                    "op": "predicate",
                    "binding": "input",
                    "expr": { "gt": [{ "column": "balance" }, 0] }
                  }
                }
              ]
            }"#,
        )
        .expect("parse live verify artifact");

        assert_eq!(artifact.constraint_set_id, "portfolio.loan_tape.v1");
        assert_eq!(artifact.report.source, "constraints.verify.json");
        assert_eq!(artifact.report.loaded, 2);
        assert!(artifact.report.hash.starts_with("sha256:"));
        assert_eq!(artifact.report.hash.len(), 71);
        assert_eq!(artifact.constraint_set.bindings[0].name, "input");
        assert_eq!(artifact.constraint_set.rules[1].check.op(), "predicate");
        assert!(artifact.batch_only_rule.is_none());
    }

    #[test]
    fn live_verify_artifact_detects_first_batch_only_rule() {
        let artifact = LiveVerifyArtifact::parse(
            "orphan_rows.verify.json",
            br#"{
              "version": "verify.constraint.v1",
              "constraint_set_id": "fixtures.query_rules.orphan_rows",
              "bindings": [
                { "name": "property", "kind": "relation" },
                { "name": "tenants", "kind": "relation" }
              ],
              "rules": [
                {
                  "id": "ORPHAN_PROPERTY_TENANT",
                  "severity": "error",
                  "portability": "batch_only",
                  "check": {
                    "op": "query_zero_rows",
                    "bindings": ["property", "tenants"],
                    "query": "SELECT 1"
                  }
                }
              ]
            }"#,
        )
        .expect("parse batch-only artifact");

        let batch_only_rule = artifact
            .batch_only_rule
            .expect("batch-only rule should be detected");
        assert_eq!(batch_only_rule.rule_id, "ORPHAN_PROPERTY_TENANT");
        assert_eq!(batch_only_rule.op, "query_zero_rows");
    }

    #[test]
    fn live_verify_artifact_requires_compiled_constraint_identity_and_rule_shape() {
        let error = LiveVerifyArtifact::parse(
            "bootstrap.verify.json",
            br#"{
              "version": "verify.constraint.v1",
              "rules": [{ "id": "TENANT_EXISTS" }]
            }"#,
        )
        .expect_err("live parser should reject underspecified artifacts");

        assert!(
            error.contains("constraint_set_id")
                || error.contains("severity")
                || error.contains("portability"),
            "unexpected error: {error}"
        );
    }
}
