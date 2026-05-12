#![forbid(unsafe_code)]

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use serde_json::Value;
use tempfile::tempdir;
use twinning::{
    catalog::parse_postgres_schema,
    cli::Engine,
    snapshot::{SnapshotRelations, TwinSnapshot, read_snapshot, restore},
};

const COMMITTED_ROWS_SCHEMA_HASH: &str =
    "sha256:0000000000000000000000000000000000000000000000000000000000000028";

fn twinning_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_twinning"))
}

fn write_schema(dir: &Path) -> PathBuf {
    let schema_path = dir.join("schema.sql");
    fs::write(
        &schema_path,
        r#"
        CREATE TABLE public.deals (
            deal_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            deal_name TEXT NOT NULL
        );

        CREATE UNIQUE INDEX deals_name_idx ON public.deals (deal_name);
        "#,
    )
    .expect("write schema");
    schema_path
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

#[test]
fn repeated_bootstrap_snapshots_keep_identical_hashes() {
    let dir = tempdir().expect("tempdir");
    let schema_path = write_schema(dir.path());
    let first_snapshot_path = dir.path().join("out").join("bootstrap-a.twin");
    let second_snapshot_path = dir.path().join("out").join("bootstrap-b.twin");

    let first = run_twinning(&[
        "postgres",
        "--schema",
        schema_path.to_str().expect("schema path"),
        "--snapshot",
        first_snapshot_path.to_str().expect("first snapshot path"),
        "--json",
    ]);
    let second = run_twinning(&[
        "postgres",
        "--schema",
        schema_path.to_str().expect("schema path"),
        "--snapshot",
        second_snapshot_path.to_str().expect("second snapshot path"),
        "--json",
    ]);

    assert_eq!(first["outcome"], "READY");
    assert_eq!(
        first["snapshot"]["snapshot_hash"],
        second["snapshot"]["snapshot_hash"]
    );

    let first_snapshot = read_snapshot(&first_snapshot_path).expect("read first snapshot");
    let second_snapshot = read_snapshot(&second_snapshot_path).expect("read second snapshot");

    assert_eq!(first_snapshot.snapshot_hash, second_snapshot.snapshot_hash);
    assert_eq!(first_snapshot.catalog, second_snapshot.catalog);
    assert_eq!(first_snapshot.table_rows, second_snapshot.table_rows);
}

#[test]
fn restoring_bootstrap_snapshot_round_trips_catalog_and_parent_hash() {
    let dir = tempdir().expect("tempdir");
    let schema_path = write_schema(dir.path());
    let original_snapshot_path = dir.path().join("bootstrap.twin");
    let restored_snapshot_path = dir.path().join("restored.twin");

    let original = run_twinning(&[
        "postgres",
        "--schema",
        schema_path.to_str().expect("schema path"),
        "--snapshot",
        original_snapshot_path
            .to_str()
            .expect("original snapshot path"),
        "--json",
    ]);
    let restored = run_twinning(&[
        "postgres",
        "--restore",
        original_snapshot_path
            .to_str()
            .expect("original snapshot path"),
        "--snapshot",
        restored_snapshot_path
            .to_str()
            .expect("restored snapshot path"),
        "--json",
    ]);

    assert_eq!(
        restored["snapshot"]["restored_from"],
        Value::String(original_snapshot_path.display().to_string())
    );
    assert_eq!(original["schema"]["hash"], restored["schema"]["hash"]);
    assert_eq!(original["catalog"], restored["catalog"]);

    let original_snapshot = read_snapshot(&original_snapshot_path).expect("read original snapshot");
    let restored_snapshot = read_snapshot(&restored_snapshot_path).expect("read restored snapshot");

    assert_eq!(restored_snapshot.catalog, original_snapshot.catalog);
    assert_eq!(restored_snapshot.table_rows, original_snapshot.table_rows);
    assert_eq!(
        restored_snapshot.base_snapshot_hash,
        Some(original_snapshot.snapshot_hash.clone())
    );
}

#[test]
fn snapshot_verification_ignores_created_at_metadata() {
    let dir = tempdir().expect("tempdir");
    let schema_path = write_schema(dir.path());
    let snapshot_path = dir.path().join("bootstrap.twin");

    run_twinning(&[
        "postgres",
        "--schema",
        schema_path.to_str().expect("schema path"),
        "--snapshot",
        snapshot_path.to_str().expect("snapshot path"),
        "--json",
    ]);

    let original_snapshot = read_snapshot(&snapshot_path).expect("read original snapshot");
    let mut json: Value =
        serde_json::from_str(&fs::read_to_string(&snapshot_path).expect("read snapshot json"))
            .expect("parse snapshot json");
    json["created_at"] = Value::String("2036-03-30T00:00:00Z".to_owned());
    fs::write(
        &snapshot_path,
        format!(
            "{}\n",
            serde_json::to_string_pretty(&json).expect("render mutated snapshot")
        ),
    )
    .expect("rewrite snapshot");

    let mutated_snapshot = read_snapshot(&snapshot_path).expect("read mutated snapshot");
    assert_eq!(
        mutated_snapshot.snapshot_hash,
        original_snapshot.snapshot_hash
    );
    assert_eq!(
        mutated_snapshot
            .compute_hash()
            .expect("compute mutated hash"),
        original_snapshot.snapshot_hash
    );
}

#[test]
fn committed_row_snapshot_canonical_bytes_survive_restore_and_refreeze() {
    let fixture_dir = committed_rows_fixture_dir();
    let schema_path = fixture_dir.join("schema.sql");
    let relations_path = fixture_dir.join("relations.json");
    let catalog = parse_postgres_schema(
        &fs::read_to_string(&schema_path).expect("read committed-row schema"),
    )
    .expect("parse committed-row schema");
    let relations: SnapshotRelations =
        serde_json::from_str(&fs::read_to_string(&relations_path).expect("read relations fixture"))
            .expect("parse relations fixture");

    let frozen = TwinSnapshot::new(
        Engine::Postgres,
        schema_path.display().to_string(),
        COMMITTED_ROWS_SCHEMA_HASH.to_owned(),
        None,
        None,
        catalog.clone(),
    )
    .expect("build frozen snapshot")
    .with_relations(relations)
    .expect("attach committed rows");
    let restored_tables = restore::restore_committed_tables(&frozen).expect("restore rows");
    let refrozen = TwinSnapshot::new(
        Engine::Postgres,
        String::from("restored-from-base-snapshot.sql"),
        COMMITTED_ROWS_SCHEMA_HASH.to_owned(),
        Some(frozen.snapshot_hash.clone()),
        None,
        catalog,
    )
    .expect("build restored snapshot")
    .with_committed_tables(restored_tables)
    .expect("refreeze restored rows");

    assert_eq!(
        frozen
            .canonical_committed_state_bytes()
            .expect("frozen canonical bytes"),
        refrozen
            .canonical_committed_state_bytes()
            .expect("refrozen canonical bytes")
    );
    assert_ne!(
        serde_json::to_vec(&frozen).expect("render frozen artifact"),
        serde_json::to_vec(&refrozen).expect("render refrozen artifact"),
        "full artifact bytes include operator metadata such as source display path and restore lineage"
    );
    assert_ne!(
        frozen.snapshot_hash, refrozen.snapshot_hash,
        "restore lineage remains explicit artifact metadata outside the committed-state byte surface"
    );
}

fn committed_rows_fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("snapshots")
        .join("committed_rows")
}
