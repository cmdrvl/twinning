use std::path::PathBuf;

use crate::{
    cli::Engine,
    config::TwinConfig,
    kernel::storage::TableStorage,
    refusal::RefusalResult,
    report::{RunReport, SchemaReport, SnapshotReport, TwinReport, TwinReportSeed},
    snapshot::{self, TwinSnapshot},
    verify_bridge::{bind_committed_relations, execute_embedded_verify},
};

#[cfg(test)]
use crate::{backend::overlay::SessionOverlayManager, catalog::Catalog, refusal};

use super::{BootstrapState, path_display, write_json};

#[cfg(test)]
const FINALIZER_SESSION_ID: &str = "__twinning_finalizer__";

#[derive(Debug, Clone, PartialEq)]
pub struct FinalizedArtifacts {
    pub report: TwinReport,
}

#[derive(Debug, Clone)]
pub struct FinalArtifactEmitter {
    engine: Engine,
    host: String,
    port: u16,
    report_path: Option<PathBuf>,
    snapshot_path: Option<PathBuf>,
    emitted: Option<FinalizedArtifacts>,
}

impl FinalArtifactEmitter {
    pub fn from_config(config: &TwinConfig) -> Self {
        Self::new(
            config.engine,
            config.host.clone(),
            config.port,
            config.report_path.clone(),
            config.snapshot_path.clone(),
        )
    }

    pub fn new(
        engine: Engine,
        host: String,
        port: u16,
        report_path: Option<PathBuf>,
        snapshot_path: Option<PathBuf>,
    ) -> Self {
        Self {
            engine,
            host,
            port,
            report_path,
            snapshot_path,
            emitted: None,
        }
    }

    pub fn emit_once(
        &mut self,
        state: &BootstrapState,
        committed_tables: Option<Vec<TableStorage>>,
        run: Option<RunReport>,
    ) -> RefusalResult<&FinalizedArtifacts> {
        if self.emitted.is_none() {
            let verify_report = resolve_verify_report(state, committed_tables.as_ref())?;
            let frozen_snapshot = freeze_snapshot(self.engine, state, committed_tables)?;
            let snapshot_report = snapshot_report_for_paths(
                &self.snapshot_path,
                state.restored_from.clone(),
                &frozen_snapshot,
            )?;
            let mut report = TwinReport::from_seed(TwinReportSeed {
                engine: self.engine.as_str(),
                host: &self.host,
                port: self.port,
                schema: SchemaReport {
                    source: state.schema_source.clone(),
                    hash: state.schema_hash.clone(),
                    table_count: state.catalog.table_count,
                    column_count: state.catalog.column_count,
                    index_count: state.catalog.index_count,
                    constraint_count: state.catalog.constraint_count,
                },
                verify_artifact: state.verify_artifact.clone(),
                verify: verify_report,
                catalog: &state.catalog,
                snapshot: snapshot_report,
                run,
                null_rates: state.null_rates.clone(),
                fk_coverage: state.fk_coverage.clone(),
                warnings: state.warnings.clone(),
            });
            apply_table_rows(&mut report, &frozen_snapshot.table_rows);

            if let Some(report_path) = &self.report_path {
                write_json(report_path, &report)?;
            }

            self.emitted = Some(FinalizedArtifacts { report });
        }

        Ok(self
            .emitted
            .as_ref()
            .expect("finalized artifacts should be cached"))
    }
}

fn resolve_verify_report(
    state: &BootstrapState,
    committed_tables: Option<&Vec<TableStorage>>,
) -> RefusalResult<Option<serde_json::Value>> {
    if let Some(verify_report) = &state.verify_report {
        return Ok(Some(verify_report.clone()));
    }

    let Some(live_verify_artifact) = &state.live_verify_artifact else {
        return Ok(None);
    };
    let Some(committed_tables) = committed_tables else {
        return Err(Box::new(crate::refusal::serialization(
            "embedded verify execution requires committed tables at the final artifact boundary",
        )));
    };

    let bindings = bind_committed_relations(&state.catalog, committed_tables.clone())?;
    Ok(Some(execute_embedded_verify(
        live_verify_artifact,
        &bindings,
    )?))
}

#[cfg(test)]
pub fn committed_tables_from_overlay(
    catalog: &Catalog,
    overlays: &SessionOverlayManager,
) -> RefusalResult<Vec<TableStorage>> {
    catalog
        .tables
        .iter()
        .map(|table| {
            overlays
                .snapshot_visible_table(FINALIZER_SESSION_ID, &table.name)
                .map_err(|error| {
                    Box::new(refusal::serialization(format!(
                        "failed to freeze committed table `{}`: {error}",
                        table.name
                    )))
                })
        })
        .collect()
}

fn freeze_snapshot(
    engine: Engine,
    state: &BootstrapState,
    committed_tables: Option<Vec<TableStorage>>,
) -> RefusalResult<TwinSnapshot> {
    let snapshot = TwinSnapshot::new(
        engine,
        state.schema_source.clone(),
        state.schema_hash.clone(),
        state.restored_snapshot_hash.clone(),
        state.verify_artifact.clone(),
        state.catalog.clone(),
    )?;

    match committed_tables {
        Some(committed_tables) => snapshot.with_committed_tables(committed_tables),
        None => Ok(snapshot),
    }
}

fn snapshot_report_for_paths(
    snapshot_path: &Option<PathBuf>,
    restored_from: Option<String>,
    snapshot: &TwinSnapshot,
) -> RefusalResult<SnapshotReport> {
    let mut snapshot_report = SnapshotReport {
        restored_from,
        written_to: None,
        snapshot_hash: None,
    };

    if let Some(snapshot_path) = snapshot_path {
        let snapshot_hash = snapshot::write_snapshot(snapshot_path, snapshot)?;
        snapshot_report.written_to = Some(path_display(snapshot_path));
        snapshot_report.snapshot_hash = Some(snapshot_hash);
    }

    Ok(snapshot_report)
}

fn apply_table_rows(report: &mut TwinReport, table_rows: &std::collections::BTreeMap<String, u64>) {
    for (table_name, row_count) in table_rows {
        if let Some(table_report) = report.tables.get_mut(table_name) {
            table_report.rows = *row_count;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;
    use tempfile::tempdir;

    use crate::{
        backend::{BaseSnapshotBackend, SessionOverlayManager},
        catalog::parse_postgres_schema,
        kernel::{storage::TableStorage, value::KernelValue},
        report::{LiveVerifyArtifact, RunReport},
    };

    use super::{BootstrapState, FinalArtifactEmitter, committed_tables_from_overlay};

    #[test]
    fn final_artifacts_omit_transient_overlay_state_from_frozen_outputs() {
        let tempdir = tempdir().expect("tempdir");
        let report_path = tempdir.path().join("out").join("final.json");
        let snapshot_path = tempdir.path().join("out").join("final.twin");
        let (state, mut overlays) = seeded_state_and_overlays();

        overlays.begin_write("writer").expect("begin writer");
        let mut writer_deals = overlays
            .snapshot_visible_table("writer", "public.deals")
            .expect("writer snapshot");
        writer_deals
            .insert_row(vec![
                KernelValue::Text(String::from("deal-2")),
                KernelValue::Text(String::from("Beta")),
            ])
            .expect("insert transient row");
        overlays
            .write_overlay_table("writer", writer_deals)
            .expect("persist transient overlay");

        let committed_tables =
            committed_tables_from_overlay(&state.catalog, &overlays).expect("freeze committed");
        let mut emitter = FinalArtifactEmitter::new(
            crate::cli::Engine::Postgres,
            String::from("127.0.0.1"),
            5432,
            Some(report_path.clone()),
            Some(snapshot_path.clone()),
        );
        let finalized = emitter
            .emit_once(&state, Some(committed_tables), None)
            .expect("emit final artifacts");

        assert_eq!(finalized.report.tables["public.deals"].rows, 1);

        let report_json: serde_json::Value =
            serde_json::from_slice(&fs::read(&report_path).expect("read report"))
                .expect("parse report");
        assert_eq!(report_json["tables"]["public.deals"]["rows"], 1);

        let snapshot_json: serde_json::Value =
            serde_json::from_slice(&fs::read(&snapshot_path).expect("read snapshot"))
                .expect("parse snapshot");
        assert_eq!(snapshot_json["table_rows"]["public.deals"], 1);
        assert_eq!(
            snapshot_json["relations"]["public.deals"]
                .as_array()
                .expect("deals rows")
                .len(),
            1
        );
    }

    #[test]
    fn rolled_back_overlay_state_is_omitted_from_final_artifacts() {
        let tempdir = tempdir().expect("tempdir");
        let snapshot_path = tempdir.path().join("out").join("final.twin");
        let (state, mut overlays) = seeded_state_and_overlays();

        overlays.begin_write("writer").expect("begin writer");
        let mut writer_deals = overlays
            .snapshot_visible_table("writer", "public.deals")
            .expect("writer snapshot");
        writer_deals
            .insert_row(vec![
                KernelValue::Text(String::from("deal-3")),
                KernelValue::Text(String::from("Gamma")),
            ])
            .expect("insert rolled-back row");
        overlays
            .write_overlay_table("writer", writer_deals)
            .expect("persist rolled-back overlay");
        overlays.rollback("writer").expect("rollback overlay");

        let committed_tables =
            committed_tables_from_overlay(&state.catalog, &overlays).expect("freeze committed");
        let mut emitter = FinalArtifactEmitter::new(
            crate::cli::Engine::Postgres,
            String::from("127.0.0.1"),
            5432,
            None,
            Some(snapshot_path.clone()),
        );
        let finalized = emitter
            .emit_once(&state, Some(committed_tables), None)
            .expect("emit final artifacts");

        assert_eq!(finalized.report.tables["public.deals"].rows, 1);

        let snapshot_json: serde_json::Value =
            serde_json::from_slice(&fs::read(&snapshot_path).expect("read snapshot"))
                .expect("parse snapshot");
        assert_eq!(snapshot_json["table_rows"]["public.deals"], 1);
    }

    #[test]
    fn final_artifact_emission_is_exactly_once_when_paths_are_configured() {
        let tempdir = tempdir().expect("tempdir");
        let report_path = tempdir.path().join("out").join("final.json");
        let snapshot_path = tempdir.path().join("out").join("final.twin");
        let (state, overlays) = seeded_state_and_overlays();
        let committed_tables =
            committed_tables_from_overlay(&state.catalog, &overlays).expect("freeze committed");

        let mut emitter = FinalArtifactEmitter::new(
            crate::cli::Engine::Postgres,
            String::from("127.0.0.1"),
            5432,
            Some(report_path.clone()),
            Some(snapshot_path.clone()),
        );

        let first = emitter
            .emit_once(&state, Some(committed_tables), None)
            .expect("first emit")
            .report
            .clone();
        let first_report_bytes = fs::read(&report_path).expect("read first report");
        let first_snapshot_bytes = fs::read(&snapshot_path).expect("read first snapshot");

        let mut changed_state = state.clone();
        changed_state.warnings = vec![String::from("must_not_overwrite")];
        let mut changed_tables = TableStorage::new(
            changed_state
                .catalog
                .table("public.deals")
                .expect("deals table"),
        )
        .expect("storage");
        changed_tables
            .insert_row(vec![
                KernelValue::Text(String::from("deal-9")),
                KernelValue::Text(String::from("Override")),
            ])
            .expect("insert changed row");

        let second = emitter
            .emit_once(&changed_state, Some(vec![changed_tables]), None)
            .expect("second emit")
            .report
            .clone();

        assert_eq!(second, first);
        assert_eq!(
            fs::read(&report_path).expect("read report again"),
            first_report_bytes
        );
        assert_eq!(
            fs::read(&snapshot_path).expect("read snapshot again"),
            first_snapshot_bytes
        );
    }

    #[test]
    fn final_artifacts_attach_run_metadata_and_switch_to_run_mode() {
        let (state, overlays) = seeded_state_and_overlays();
        let committed_tables =
            committed_tables_from_overlay(&state.catalog, &overlays).expect("freeze committed");
        let mut emitter = FinalArtifactEmitter::new(
            crate::cli::Engine::Postgres,
            String::from("127.0.0.1"),
            5432,
            None,
            None,
        );

        let finalized = emitter
            .emit_once(
                &state,
                Some(committed_tables),
                Some(RunReport {
                    command: String::from("python extract.py"),
                    exit_code: None,
                    signal: Some(15),
                    timed_out: true,
                }),
            )
            .expect("emit final artifacts");

        assert_eq!(finalized.report.mode, "run_once");
        assert_eq!(finalized.report.outcome, "FAIL");
        assert_eq!(
            finalized.report.run,
            Some(RunReport {
                command: String::from("python extract.py"),
                exit_code: None,
                signal: Some(15),
                timed_out: true,
            })
        );
    }

    #[test]
    fn final_artifacts_preserve_verify_payload_and_twin_native_metrics() {
        let tempdir = tempdir().expect("tempdir");
        let report_path = tempdir.path().join("out").join("final.json");
        let (mut state, overlays) = seeded_state_and_overlays();
        state.verify_report = Some(json!({
            "version": "verify.report.v1",
            "outcome": "PASS",
            "summary": {
                "total_rules": 2,
                "passed_rules": 2,
                "failed_rules": 0
            }
        }));
        state.null_rates = Some(crate::report::RatioMap::from([(
            String::from("public.deals.deal_name"),
            0.25,
        )]));
        state.fk_coverage = Some(crate::report::RatioMap::from([(
            String::from("public.deals.deal_id -> public.deals.deal_id"),
            1.0,
        )]));

        let committed_tables =
            committed_tables_from_overlay(&state.catalog, &overlays).expect("freeze committed");
        let mut emitter = FinalArtifactEmitter::new(
            crate::cli::Engine::Postgres,
            String::from("127.0.0.1"),
            5432,
            Some(report_path.clone()),
            None,
        );

        let finalized = emitter
            .emit_once(&state, Some(committed_tables), None)
            .expect("emit final artifacts");

        assert_eq!(
            finalized.report.verify,
            Some(json!({
                "version": "verify.report.v1",
                "outcome": "PASS",
                "summary": {
                    "total_rules": 2,
                    "passed_rules": 2,
                    "failed_rules": 0
                }
            }))
        );
        assert_eq!(
            finalized.report.null_rates,
            Some(crate::report::RatioMap::from([(
                String::from("public.deals.deal_name"),
                0.25,
            )]))
        );
        assert_eq!(
            finalized.report.fk_coverage,
            Some(crate::report::RatioMap::from([(
                String::from("public.deals.deal_id -> public.deals.deal_id"),
                1.0,
            )]))
        );

        let report_json: serde_json::Value =
            serde_json::from_slice(&fs::read(&report_path).expect("read report"))
                .expect("parse report");
        assert_eq!(report_json["verify"]["version"], "verify.report.v1");
        assert_eq!(report_json["verify"]["summary"]["failed_rules"], 0);
        assert_eq!(report_json["null_rates"]["public.deals.deal_name"], 0.25);
        assert_eq!(
            report_json["fk_coverage"]["public.deals.deal_id -> public.deals.deal_id"],
            1.0
        );
    }

    #[test]
    fn final_artifacts_execute_embedded_verify_from_live_artifact() {
        let (mut state, overlays) = seeded_state_and_overlays();
        state.live_verify_artifact = Some(
            LiveVerifyArtifact::parse(
                "constraints.verify.json",
                br#"{
                  "version": "verify.constraint.v1",
                  "constraint_set_id": "fixtures.public.deals.not_null",
                  "bindings": [
                    { "name": "public.deals", "kind": "relation", "key_fields": ["deal_id"] }
                  ],
                  "rules": [
                    {
                      "id": "DEAL_NAME_PRESENT",
                      "severity": "error",
                      "portability": "portable",
                      "check": { "op": "not_null", "binding": "public.deals", "columns": ["deal_name"] }
                    }
                  ]
                }"#,
            )
            .expect("parse verify artifact"),
        );

        let committed_tables =
            committed_tables_from_overlay(&state.catalog, &overlays).expect("freeze committed");
        let mut emitter = FinalArtifactEmitter::new(
            crate::cli::Engine::Postgres,
            String::from("127.0.0.1"),
            5432,
            None,
            None,
        );

        let finalized = emitter
            .emit_once(&state, Some(committed_tables), None)
            .expect("emit final artifacts");

        assert_eq!(
            finalized.report.verify.as_ref().expect("verify payload")["tool"],
            "verify"
        );
        assert_eq!(
            finalized.report.verify.as_ref().expect("verify payload")["execution_mode"],
            "embedded"
        );
        assert_eq!(
            finalized.report.verify.as_ref().expect("verify payload")["summary"]["failed_rules"],
            0
        );
        assert!(finalized.report.verify.as_ref().expect("verify payload")["refusal"].is_null());
    }

    #[test]
    fn final_artifacts_cache_embedded_verify_payload_after_first_emit() {
        let (mut state, overlays) = seeded_state_and_overlays();
        state.live_verify_artifact = Some(
            LiveVerifyArtifact::parse(
                "constraints.verify.json",
                br#"{
                  "version": "verify.constraint.v1",
                  "constraint_set_id": "fixtures.public.deals.row_count",
                  "bindings": [
                    { "name": "public.deals", "kind": "relation", "key_fields": ["deal_id"] }
                  ],
                  "rules": [
                    {
                      "id": "DEALS_PRESENT",
                      "severity": "error",
                      "portability": "portable",
                      "check": { "op": "row_count", "binding": "public.deals", "compare": { "gte": 1 } }
                    }
                  ]
                }"#,
            )
            .expect("parse verify artifact"),
        );

        let committed_tables =
            committed_tables_from_overlay(&state.catalog, &overlays).expect("freeze committed");
        let mut emitter = FinalArtifactEmitter::new(
            crate::cli::Engine::Postgres,
            String::from("127.0.0.1"),
            5432,
            None,
            None,
        );

        let first = emitter
            .emit_once(&state, Some(committed_tables), None)
            .expect("first emit")
            .report
            .clone();
        assert_eq!(
            first.verify.as_ref().expect("verify payload")["summary"]["failed_rules"],
            0
        );

        let empty_deals =
            TableStorage::new(state.catalog.table("public.deals").expect("deals table"))
                .expect("empty deals table");

        let second = emitter
            .emit_once(&state, Some(vec![empty_deals]), None)
            .expect("second emit")
            .report
            .clone();

        assert_eq!(second.verify, first.verify);
        assert_eq!(
            second.verify.as_ref().expect("verify payload")["summary"]["failed_rules"],
            0
        );
    }

    fn seeded_state_and_overlays() -> (BootstrapState, SessionOverlayManager) {
        let schema_source = String::from("fixtures/schema.sql");
        let schema_hash = String::from("sha256:seeded");
        let catalog = parse_postgres_schema(
            r#"
            CREATE TABLE public.deals (
                deal_id TEXT PRIMARY KEY,
                deal_name TEXT NOT NULL
            );
            "#,
        )
        .expect("parse schema");
        let mut deals = TableStorage::new(catalog.table("public.deals").expect("deals table"))
            .expect("deals storage");
        deals
            .insert_row(vec![
                KernelValue::Text(String::from("deal-1")),
                KernelValue::Text(String::from("Alpha")),
            ])
            .expect("insert committed row");

        let overlays = SessionOverlayManager::new(
            BaseSnapshotBackend::new([deals]).expect("build committed backend"),
        );

        (
            BootstrapState {
                schema_source,
                schema_hash,
                verify_artifact: None,
                verify_report: None,
                live_verify_artifact: None,
                catalog,
                restored_from: None,
                restored_snapshot_hash: None,
                null_rates: None,
                fk_coverage: None,
                warnings: Vec::new(),
            },
            overlays,
        )
    }
}
