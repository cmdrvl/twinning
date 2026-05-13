#![forbid(unsafe_code)]

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::Instant,
};

use serde::Serialize;
use serde_json::Value;
use tempfile::tempdir;
use twinning::{
    backend::SessionOverlayManager,
    catalog::parse_postgres_schema,
    cli::Engine,
    kernel::value::KernelValue,
    snapshot::{SnapshotRelations, TwinSnapshot, read_snapshot, restore, write_snapshot},
};

const SAMPLE_COUNT: usize = 3;
const OVERLAY_WORKLOAD_ROWS: usize = 512;
const TOURNAMENT_BASE_ROWS: usize = 32;
const TOURNAMENT_CYCLES: usize = 8;
const BASELINE_VERSION: &str = "twinning.storage-budget-baseline.v1";
const BUDGET_SCHEMA: &str = r#"
CREATE TABLE public.deals (
    deal_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    deal_name TEXT NOT NULL
);

CREATE UNIQUE INDEX deals_name_idx ON public.deals (deal_name);
"#;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BudgetDefinition {
    metric: &'static str,
    label: &'static str,
    unit: &'static str,
    target: u64,
    red_line: u64,
}

const BUDGET_DEFINITIONS: [BudgetDefinition; 5] = [
    BudgetDefinition {
        metric: "cold_start_from_schema",
        label: "Cold startup from schema",
        unit: "ms",
        target: 1_000,
        red_line: 2_000,
    },
    BudgetDefinition {
        metric: "warm_restore_from_snapshot",
        label: "Warm restore from base snapshot",
        unit: "ms",
        target: 2_000,
        red_line: 5_000,
    },
    BudgetDefinition {
        metric: "reset_to_clean_overlay",
        label: "Reset to clean overlay",
        unit: "ms",
        target: 200,
        red_line: 500,
    },
    BudgetDefinition {
        metric: "idle_private_rss_kib",
        label: "Idle private RSS per tournament twin",
        unit: "KiB",
        target: 128 * 1024,
        red_line: 256 * 1024,
    },
    BudgetDefinition {
        metric: "workload_private_rss_kib",
        label: "Private RSS under reference overlay workload",
        unit: "KiB",
        target: 256 * 1024,
        red_line: 512 * 1024,
    },
];

const TOURNAMENT_BUDGET_DEFINITIONS: [BudgetDefinition; 4] = [
    BudgetDefinition {
        metric: "tournament_startup_from_base_snapshot",
        label: "Tournament startup from shared base snapshot",
        unit: "ms",
        target: 2_000,
        red_line: 5_000,
    },
    BudgetDefinition {
        metric: "tournament_reset_to_base_snapshot",
        label: "Tournament reset to shared base snapshot",
        unit: "ms",
        target: 200,
        red_line: 500,
    },
    BudgetDefinition {
        metric: "overlay_rows_per_candidate",
        label: "Overlay rows per tournament candidate",
        unit: "rows",
        target: (TOURNAMENT_BASE_ROWS + OVERLAY_WORKLOAD_ROWS) as u64,
        red_line: (TOURNAMENT_BASE_ROWS + OVERLAY_WORKLOAD_ROWS) as u64,
    },
    BudgetDefinition {
        metric: "tournament_rss_growth_kib",
        label: "RSS growth across tournament reset loop",
        unit: "KiB",
        target: 32 * 1024,
        red_line: 64 * 1024,
    },
];

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct BudgetCapture {
    version: &'static str,
    phase: &'static str,
    metrics: Vec<MetricCapture>,
}

impl BudgetCapture {
    fn phase_zero(
        cold_start: CommandTiming,
        warm_restore: CommandTiming,
        reset_overlay: Vec<u64>,
        idle_private_rss: Option<Vec<u64>>,
        workload_private_rss: Option<Vec<u64>>,
    ) -> Self {
        Self {
            version: BASELINE_VERSION,
            phase: "phase0_bootstrap",
            metrics: vec![
                MetricCapture::captured(BUDGET_DEFINITIONS[0], cold_start.samples_ms),
                MetricCapture::captured(BUDGET_DEFINITIONS[1], warm_restore.samples_ms),
                MetricCapture::captured(BUDGET_DEFINITIONS[2], reset_overlay),
                MetricCapture::captured_or_unavailable(
                    BUDGET_DEFINITIONS[3],
                    idle_private_rss,
                    "private RSS capture requires `ps` support on the local platform",
                ),
                MetricCapture::captured_or_unavailable(
                    BUDGET_DEFINITIONS[4],
                    workload_private_rss,
                    "private RSS capture requires `ps` support on the local platform",
                ),
            ],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct MetricCapture {
    metric: &'static str,
    label: &'static str,
    unit: &'static str,
    target: u64,
    red_line: u64,
    samples: Vec<u64>,
    median: Option<u64>,
    status: &'static str,
    note: Option<&'static str>,
}

impl MetricCapture {
    fn captured(definition: BudgetDefinition, samples: Vec<u64>) -> Self {
        Self {
            metric: definition.metric,
            label: definition.label,
            unit: definition.unit,
            target: definition.target,
            red_line: definition.red_line,
            median: median(&samples),
            samples,
            status: "captured",
            note: None,
        }
    }

    fn captured_or_unavailable(
        definition: BudgetDefinition,
        samples: Option<Vec<u64>>,
        note: &'static str,
    ) -> Self {
        match samples {
            Some(samples) => Self::captured(definition, samples),
            None => Self::unavailable(definition, note),
        }
    }

    fn unavailable(definition: BudgetDefinition, note: &'static str) -> Self {
        Self {
            metric: definition.metric,
            label: definition.label,
            unit: definition.unit,
            target: definition.target,
            red_line: definition.red_line,
            samples: Vec::new(),
            median: None,
            status: "unavailable_platform",
            note: Some(note),
        }
    }
}

#[derive(Debug, Clone)]
struct CommandTiming {
    samples_ms: Vec<u64>,
    last_output: Value,
}

fn twinning_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_twinning"))
}

fn write_schema(dir: &Path) -> PathBuf {
    let schema_path = dir.join("schema.sql");
    fs::write(&schema_path, BUDGET_SCHEMA).expect("write schema");
    schema_path
}

fn write_tournament_snapshot(dir: &Path) -> (PathBuf, TwinSnapshot) {
    let catalog = parse_postgres_schema(BUDGET_SCHEMA).expect("parse budget schema");
    let relations = SnapshotRelations::from([(
        String::from("public.deals"),
        (0..TOURNAMENT_BASE_ROWS)
            .map(|ordinal| {
                BTreeMap::from([
                    (
                        String::from("deal_id"),
                        serde_json::json!({
                            "kind": "text",
                            "value": format!("base-deal-{ordinal:04}")
                        }),
                    ),
                    (
                        String::from("tenant_id"),
                        serde_json::json!({
                            "kind": "text",
                            "value": "tenant-a"
                        }),
                    ),
                    (
                        String::from("deal_name"),
                        serde_json::json!({
                            "kind": "text",
                            "value": format!("Base Deal {ordinal:04}")
                        }),
                    ),
                ])
            })
            .collect(),
    )]);
    let snapshot = TwinSnapshot::new(
        Engine::Postgres,
        String::from("tests/fixtures/storage/tournament_budget/schema.sql"),
        String::from("sha256:tournament-budget-schema"),
        None,
        None,
        catalog,
    )
    .expect("build tournament snapshot")
    .with_relations(relations)
    .expect("seed tournament relations");

    let snapshot_path = dir.join("tournament-base.twin");
    write_snapshot(&snapshot_path, &snapshot).expect("write tournament snapshot");
    (snapshot_path, snapshot)
}

fn run_twinning(args: &[&str]) -> Value {
    let output = Command::new(twinning_bin())
        .args(args)
        .output()
        .expect("run twinning");

    assert!(
        output.status.success(),
        "twinning exited unsuccessfully: status={:?}, stdout={}, stderr={}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    serde_json::from_slice(&output.stdout).expect("parse twinning json output")
}

fn measure_command(args: &[&str], iterations: usize) -> CommandTiming {
    assert!(iterations > 0, "iterations must be positive");

    let mut samples_ms = Vec::with_capacity(iterations);
    let mut last_output = None;

    for _ in 0..iterations {
        let started_at = Instant::now();
        let output = Command::new(twinning_bin())
            .args(args)
            .output()
            .expect("run twinning");
        let elapsed_ms = started_at.elapsed().as_millis().max(1) as u64;

        assert!(
            output.status.success(),
            "twinning exited unsuccessfully: status={:?}, stdout={}, stderr={}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        samples_ms.push(elapsed_ms);
        last_output =
            Some(serde_json::from_slice(&output.stdout).expect("parse twinning json output"));
    }

    CommandTiming {
        samples_ms,
        last_output: last_output.expect("timing output"),
    }
}

fn measure_final_artifact_emission(
    snapshot_path: &Path,
    dir: &Path,
    iterations: usize,
) -> CommandTiming {
    assert!(iterations > 0, "iterations must be positive");

    let mut samples_ms = Vec::with_capacity(iterations);
    let mut last_output = None;

    for iteration in 0..iterations {
        let report_path = dir.join(format!("tournament-final-{iteration}.json"));
        let final_snapshot_path = dir.join(format!("tournament-final-{iteration}.twin"));
        let args = vec![
            String::from("postgres"),
            String::from("--restore"),
            snapshot_path.display().to_string(),
            String::from("--port"),
            String::from("0"),
            String::from("--run"),
            String::from("exit 0"),
            String::from("--report"),
            report_path.display().to_string(),
            String::from("--snapshot"),
            final_snapshot_path.display().to_string(),
            String::from("--json"),
        ];

        let started_at = Instant::now();
        let output = Command::new(twinning_bin())
            .args(&args)
            .output()
            .expect("run tournament final artifact emission");
        let elapsed_ms = started_at.elapsed().as_millis().max(1) as u64;

        assert!(
            output.status.success(),
            "twinning final artifact emission exited unsuccessfully: status={:?}, stdout={}, stderr={}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let rendered: Value =
            serde_json::from_slice(&output.stdout).expect("parse tournament final report");
        assert_final_artifacts_preserve_base_state(snapshot_path, &final_snapshot_path, &rendered);
        let written_report: Value =
            serde_json::from_slice(&fs::read(&report_path).expect("read tournament report"))
                .expect("parse written tournament report");
        assert_eq!(written_report, rendered);

        samples_ms.push(elapsed_ms);
        last_output = Some(rendered);
    }

    CommandTiming {
        samples_ms,
        last_output: last_output.expect("timing output"),
    }
}

fn median(samples: &[u64]) -> Option<u64> {
    if samples.is_empty() {
        return None;
    }

    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    Some(sorted[sorted.len() / 2])
}

fn measure_overlay_reset(snapshot_path: &Path, iterations: usize) -> Vec<u64> {
    assert!(iterations > 0, "iterations must be positive");

    let snapshot = read_snapshot(snapshot_path).expect("read snapshot");
    let mut samples_ms = Vec::with_capacity(iterations);

    for iteration in 0..iterations {
        let mut overlays = restore::restore_overlay_manager(&snapshot).expect("restore overlays");
        seed_overlay_workload(&mut overlays, iteration * OVERLAY_WORKLOAD_ROWS);

        let started_at = Instant::now();
        restore::reset_overlay_manager(&snapshot, &mut overlays).expect("reset overlays");
        let elapsed_ms = started_at.elapsed().as_millis().max(1) as u64;

        assert!(overlays.writer_session_id().is_none());
        assert_eq!(
            overlays
                .visible_table("reader", "public.deals")
                .expect("reader view after reset")
                .row_count(),
            0
        );

        samples_ms.push(elapsed_ms);
    }

    samples_ms
}

fn measure_tournament_startup(snapshot_path: &Path, iterations: usize) -> Vec<u64> {
    assert!(iterations > 0, "iterations must be positive");

    let mut samples_ms = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started_at = Instant::now();
        let snapshot = read_snapshot(snapshot_path).expect("read tournament snapshot");
        let overlays = restore::restore_overlay_manager(&snapshot).expect("restore overlays");
        let elapsed_ms = started_at.elapsed().as_millis().max(1) as u64;

        assert_eq!(
            overlays
                .visible_table("reader", "public.deals")
                .expect("reader base view")
                .row_count(),
            TOURNAMENT_BASE_ROWS
        );
        samples_ms.push(elapsed_ms);
    }

    samples_ms
}

fn exercise_tournament_reset_loop(snapshot_path: &Path) -> TournamentResetCapture {
    let snapshot = read_snapshot(snapshot_path).expect("read tournament snapshot");
    let mut overlays = restore::restore_overlay_manager(&snapshot).expect("restore overlays");
    let rss_before = sample_private_rss_kib();
    let mut reset_samples_ms = Vec::with_capacity(TOURNAMENT_CYCLES);
    let mut overlay_row_samples = Vec::with_capacity(TOURNAMENT_CYCLES);

    for iteration in 0..TOURNAMENT_CYCLES {
        seed_overlay_workload(&mut overlays, iteration * OVERLAY_WORKLOAD_ROWS);
        let overlay_rows = overlays
            .visible_table("writer", "public.deals")
            .expect("writer overlay view")
            .row_count() as u64;
        overlay_row_samples.push(overlay_rows);

        let started_at = Instant::now();
        restore::reset_overlay_manager(&snapshot, &mut overlays).expect("reset overlays");
        reset_samples_ms.push(started_at.elapsed().as_millis().max(1) as u64);

        assert!(overlays.writer_session_id().is_none());
        assert_eq!(
            overlays
                .visible_table("reader", "public.deals")
                .expect("reader base view after reset")
                .row_count(),
            TOURNAMENT_BASE_ROWS
        );
        assert_eq!(
            snapshot_hash_from_overlay(&snapshot, &overlays),
            snapshot.snapshot_hash,
            "reset must restore the exact base snapshot hash"
        );
    }

    let rss_growth_kib = match (rss_before, sample_private_rss_kib()) {
        (Some(before), Some(after)) => Some(vec![after.saturating_sub(before)]),
        _ => None,
    };

    TournamentResetCapture {
        reset_samples_ms,
        overlay_row_samples,
        rss_growth_kib,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TournamentResetCapture {
    reset_samples_ms: Vec<u64>,
    overlay_row_samples: Vec<u64>,
    rss_growth_kib: Option<Vec<u64>>,
}

fn capture_private_rss_kib(
    snapshot_path: &Path,
    iterations: usize,
    apply_overlay_workload: bool,
) -> Option<Vec<u64>> {
    assert!(iterations > 0, "iterations must be positive");

    let snapshot = read_snapshot(snapshot_path).expect("read snapshot");
    let mut samples = Vec::with_capacity(iterations);

    for iteration in 0..iterations {
        let mut overlays = restore::restore_overlay_manager(&snapshot).expect("restore overlays");
        if apply_overlay_workload {
            seed_overlay_workload(&mut overlays, iteration * OVERLAY_WORKLOAD_ROWS);
        }

        let rss_kib = sample_private_rss_kib()?;
        std::hint::black_box(&overlays);
        samples.push(rss_kib);
    }

    Some(samples)
}

fn sample_private_rss_kib() -> Option<u64> {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }

    String::from_utf8(output.stdout)
        .ok()?
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()
}

fn seed_overlay_workload(overlays: &mut SessionOverlayManager, offset: usize) {
    overlays.begin_write("writer").expect("begin writer");
    let mut overlay = overlays
        .snapshot_visible_table("writer", "public.deals")
        .expect("clone visible deals");

    for ordinal in 0..OVERLAY_WORKLOAD_ROWS {
        let sequence = offset + ordinal;
        overlay
            .insert_row(vec![
                KernelValue::Text(format!("deal-{sequence:04}")),
                KernelValue::Text(String::from("tenant-a")),
                KernelValue::Text(format!("Deal {sequence:04}")),
            ])
            .expect("insert overlay workload row");
    }

    overlays
        .write_overlay_table("writer", overlay)
        .expect("write overlay table");
}

fn snapshot_hash_from_overlay(
    base_snapshot: &TwinSnapshot,
    overlays: &SessionOverlayManager,
) -> String {
    let committed_tables = base_snapshot
        .catalog
        .tables
        .iter()
        .map(|table| {
            overlays
                .snapshot_visible_table("reader", &table.name)
                .unwrap_or_else(|error| panic!("freeze `{}` after reset: {error}", table.name))
        })
        .collect::<Vec<_>>();

    TwinSnapshot::new(
        base_snapshot.engine,
        base_snapshot.schema_source.clone(),
        base_snapshot.schema_hash.clone(),
        base_snapshot.base_snapshot_hash.clone(),
        base_snapshot.verify_artifact.clone(),
        base_snapshot.catalog.clone(),
    )
    .expect("build reset snapshot")
    .with_catalog_declaration(base_snapshot.catalog_declaration.clone())
    .expect("apply catalog declaration")
    .with_source_materialization(base_snapshot.source_materialization.clone())
    .expect("apply source materialization")
    .with_committed_tables(committed_tables)
    .expect("freeze reset committed tables")
    .snapshot_hash
}

fn assert_final_artifacts_preserve_base_state(
    base_snapshot_path: &Path,
    final_snapshot_path: &Path,
    report: &Value,
) {
    let base_snapshot = read_snapshot(base_snapshot_path).expect("read base snapshot");
    let final_snapshot = read_snapshot(final_snapshot_path).expect("read final snapshot");

    assert_eq!(report["version"], "twinning.v0");
    assert_eq!(report["mode"], "run_once");
    assert_eq!(report["outcome"], "READY");
    assert_eq!(report["run"]["exit_code"], 0);
    assert_eq!(
        report["snapshot"]["restored_from"],
        Value::String(base_snapshot_path.display().to_string())
    );
    assert_eq!(
        report["snapshot"]["snapshot_hash"],
        Value::String(final_snapshot.snapshot_hash.clone())
    );
    assert_eq!(
        final_snapshot.base_snapshot_hash.as_deref(),
        Some(base_snapshot.snapshot_hash.as_str())
    );
    assert_eq!(
        final_snapshot
            .canonical_committed_state_bytes()
            .expect("final committed state bytes"),
        base_snapshot
            .canonical_committed_state_bytes()
            .expect("base committed state bytes"),
        "final artifacts must preserve committed base state when the tournament child makes no writes"
    );
    assert_eq!(
        report["tables"]["public.deals"]["rows"],
        TOURNAMENT_BASE_ROWS as u64
    );
}

#[test]
fn budget_metric_definitions_match_plan_contract() {
    assert_eq!(
        BUDGET_DEFINITIONS,
        [
            BudgetDefinition {
                metric: "cold_start_from_schema",
                label: "Cold startup from schema",
                unit: "ms",
                target: 1_000,
                red_line: 2_000,
            },
            BudgetDefinition {
                metric: "warm_restore_from_snapshot",
                label: "Warm restore from base snapshot",
                unit: "ms",
                target: 2_000,
                red_line: 5_000,
            },
            BudgetDefinition {
                metric: "reset_to_clean_overlay",
                label: "Reset to clean overlay",
                unit: "ms",
                target: 200,
                red_line: 500,
            },
            BudgetDefinition {
                metric: "idle_private_rss_kib",
                label: "Idle private RSS per tournament twin",
                unit: "KiB",
                target: 128 * 1024,
                red_line: 256 * 1024,
            },
            BudgetDefinition {
                metric: "workload_private_rss_kib",
                label: "Private RSS under reference overlay workload",
                unit: "KiB",
                target: 256 * 1024,
                red_line: 512 * 1024,
            },
        ]
    );
}

#[test]
fn phase_zero_budget_harness_captures_storage_and_rss_baselines() {
    let dir = tempdir().expect("tempdir");
    let schema_path = write_schema(dir.path());
    let snapshot_path = dir.path().join("bootstrap.twin");

    let cold_start = measure_command(
        &[
            "postgres",
            "--schema",
            schema_path.to_str().expect("schema path"),
            "--json",
        ],
        SAMPLE_COUNT,
    );

    let seeded_snapshot = run_twinning(&[
        "postgres",
        "--schema",
        schema_path.to_str().expect("schema path"),
        "--snapshot",
        snapshot_path.to_str().expect("snapshot path"),
        "--json",
    ]);
    assert_eq!(seeded_snapshot["outcome"], "READY");

    let warm_restore = measure_command(
        &[
            "postgres",
            "--restore",
            snapshot_path.to_str().expect("snapshot path"),
            "--json",
        ],
        SAMPLE_COUNT,
    );

    let reset_overlay = measure_overlay_reset(&snapshot_path, SAMPLE_COUNT);
    let idle_private_rss = capture_private_rss_kib(&snapshot_path, SAMPLE_COUNT, false);
    let workload_private_rss = capture_private_rss_kib(&snapshot_path, SAMPLE_COUNT, true);

    let baseline = BudgetCapture::phase_zero(
        cold_start.clone(),
        warm_restore.clone(),
        reset_overlay,
        idle_private_rss,
        workload_private_rss,
    );
    let rendered = serde_json::to_value(&baseline).expect("serialize budget capture");

    assert_eq!(baseline.version, BASELINE_VERSION);
    assert_eq!(baseline.phase, "phase0_bootstrap");
    assert_eq!(baseline.metrics.len(), 5);

    assert_eq!(cold_start.last_output["outcome"], "READY");
    assert_eq!(cold_start.last_output["mode"], "bootstrap");
    assert_eq!(warm_restore.last_output["outcome"], "READY");
    assert_eq!(warm_restore.last_output["mode"], "bootstrap");
    assert_eq!(
        warm_restore.last_output["snapshot"]["restored_from"],
        Value::String(snapshot_path.display().to_string())
    );

    assert_eq!(baseline.metrics[0].metric, "cold_start_from_schema");
    assert_eq!(baseline.metrics[0].status, "captured");
    assert_eq!(baseline.metrics[1].metric, "warm_restore_from_snapshot");
    assert_eq!(baseline.metrics[1].status, "captured");
    assert_eq!(baseline.metrics[2].metric, "reset_to_clean_overlay");
    assert_eq!(baseline.metrics[2].status, "captured");
    assert_eq!(baseline.metrics[3].metric, "idle_private_rss_kib");
    assert_metric_capture_or_platform_note(&baseline.metrics[3])
        .expect("private RSS metric status should stay explicit");
    assert_eq!(baseline.metrics[4].metric, "workload_private_rss_kib");
    assert_metric_capture_or_platform_note(&baseline.metrics[4])
        .expect("shared RSS metric status should stay explicit");
    assert_metrics_within_red_lines(&baseline.metrics);
    assert!(
        baseline.metrics[0]
            .samples
            .iter()
            .all(|sample| *sample >= 1),
        "cold-start baseline samples should be captured"
    );
    assert!(
        baseline.metrics[1]
            .samples
            .iter()
            .all(|sample| *sample >= 1),
        "warm-restore baseline samples should be captured"
    );
    assert!(
        baseline.metrics[2]
            .samples
            .iter()
            .all(|sample| *sample >= 1),
        "reset baseline samples should be captured"
    );

    assert_eq!(rendered["metrics"][0]["target"], 1_000);
    assert_eq!(rendered["metrics"][0]["red_line"], 2_000);
    assert_eq!(rendered["metrics"][1]["target"], 2_000);
    assert_eq!(rendered["metrics"][1]["red_line"], 5_000);
    assert_eq!(rendered["metrics"][2]["target"], 200);
    assert_eq!(rendered["metrics"][2]["red_line"], 500);
    assert_eq!(rendered["metrics"][3]["target"], 128 * 1024);
    assert_eq!(rendered["metrics"][3]["red_line"], 256 * 1024);
    assert_eq!(rendered["metrics"][4]["target"], 256 * 1024);
    assert_eq!(rendered["metrics"][4]["red_line"], 512 * 1024);

    eprintln!(
        "{}",
        serde_json::to_string_pretty(&baseline).expect("render budget baseline")
    );
}

#[test]
fn tournament_workload_gate_enforces_reset_overlay_memory_and_final_artifacts() {
    let dir = tempdir().expect("tempdir");
    let (snapshot_path, base_snapshot) = write_tournament_snapshot(dir.path());

    let startup_samples = measure_tournament_startup(&snapshot_path, SAMPLE_COUNT);
    let reset_capture = exercise_tournament_reset_loop(&snapshot_path);
    let final_artifacts = measure_final_artifact_emission(&snapshot_path, dir.path(), SAMPLE_COUNT);

    assert_eq!(
        read_snapshot(&snapshot_path)
            .expect("read tournament snapshot after workload")
            .snapshot_hash,
        base_snapshot.snapshot_hash,
        "tournament reset loop must not mutate the shared base snapshot"
    );
    assert_eq!(final_artifacts.last_output["mode"], "run_once");
    assert_eq!(final_artifacts.last_output["outcome"], "READY");

    let gate = BudgetCapture {
        version: BASELINE_VERSION,
        phase: "tournament_workload_gate",
        metrics: vec![
            MetricCapture::captured(TOURNAMENT_BUDGET_DEFINITIONS[0], startup_samples),
            MetricCapture::captured(
                TOURNAMENT_BUDGET_DEFINITIONS[1],
                reset_capture.reset_samples_ms,
            ),
            MetricCapture::captured(
                TOURNAMENT_BUDGET_DEFINITIONS[2],
                reset_capture.overlay_row_samples,
            ),
            MetricCapture::captured_or_unavailable(
                TOURNAMENT_BUDGET_DEFINITIONS[3],
                reset_capture.rss_growth_kib,
                "RSS growth capture requires `ps` support on the local platform; CI treats this as an explicit platform skip, not a warning",
            ),
            MetricCapture::captured(
                BudgetDefinition {
                    metric: "final_artifact_emission",
                    label: "Final report and snapshot emission",
                    unit: "ms",
                    target: 1_000,
                    red_line: 2_000,
                },
                final_artifacts.samples_ms,
            ),
        ],
    };

    assert_metrics_within_red_lines(&gate.metrics);
    eprintln!(
        "{}",
        serde_json::to_string_pretty(&gate).expect("render tournament budget gate")
    );
}

fn assert_metric_capture_or_platform_note(metric: &MetricCapture) -> Result<(), String> {
    match metric.status {
        "captured" => {
            assert!(
                metric.samples.iter().all(|sample| *sample > 0),
                "captured metrics should record positive samples"
            );
            Ok(())
        }
        "unavailable_platform" => {
            assert!(metric.samples.is_empty());
            assert!(metric.note.is_some());
            Ok(())
        }
        other => Err(format!("unexpected metric status `{other}`")),
    }
}

fn assert_metrics_within_red_lines(metrics: &[MetricCapture]) {
    for metric in metrics {
        match metric.status {
            "captured" => {
                assert!(
                    !metric.samples.is_empty(),
                    "captured metric `{}` must include at least one sample",
                    metric.metric
                );
                assert!(
                    metric
                        .samples
                        .iter()
                        .all(|sample| *sample <= metric.red_line),
                    "budget red line exceeded for `{}`: red_line={} {}, samples={:?}",
                    metric.metric,
                    metric.red_line,
                    metric.unit,
                    metric.samples
                );
            }
            "unavailable_platform" => {
                assert!(
                    metric.note.is_some(),
                    "platform-skipped metric `{}` must document why it was skipped",
                    metric.metric
                );
            }
            other => panic!("unexpected metric status `{other}` for `{}`", metric.metric),
        }
    }
}
