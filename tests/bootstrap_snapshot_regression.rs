#![forbid(unsafe_code)]

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use serde_json::Value;
use tempfile::tempdir;
use twinning::snapshot::read_snapshot;

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
