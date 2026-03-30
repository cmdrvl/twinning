#![forbid(unsafe_code)]

use std::{fs, path::PathBuf};

use tempfile::tempdir;
use twinning::{
    backend::Backend,
    catalog::parse_postgres_schema,
    cli::Engine,
    kernel::value::KernelValue,
    snapshot::{TwinSnapshot, read_snapshot, restore},
};

fn seeded_snapshot_path() -> PathBuf {
    let dir = tempdir().expect("tempdir");
    let path = dir.path().join("seed.twin");
    let snapshot = TwinSnapshot::new(
        Engine::Postgres,
        String::from("seed.sql"),
        String::from("sha256:seed"),
        None,
        None,
        parse_postgres_schema(
            r#"
            CREATE TABLE public.deals (
                deal_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                deal_name TEXT NOT NULL
            );
            "#,
        )
        .expect("parse schema"),
    )
    .expect("snapshot")
    .with_relations(std::collections::BTreeMap::from([(
        String::from("public.deals"),
        vec![
            std::collections::BTreeMap::from([
                (
                    String::from("deal_id"),
                    serde_json::json!({ "kind": "text", "value": "deal-2" }),
                ),
                (
                    String::from("tenant_id"),
                    serde_json::json!({ "kind": "text", "value": "tenant-2" }),
                ),
                (
                    String::from("deal_name"),
                    serde_json::json!({ "kind": "text", "value": "Beta" }),
                ),
            ]),
            std::collections::BTreeMap::from([
                (
                    String::from("deal_id"),
                    serde_json::json!({ "kind": "text", "value": "deal-1" }),
                ),
                (
                    String::from("tenant_id"),
                    serde_json::json!({ "kind": "text", "value": "tenant-1" }),
                ),
                (
                    String::from("deal_name"),
                    serde_json::json!({ "kind": "text", "value": "Alpha" }),
                ),
            ]),
        ],
    )]))
    .expect("seed relations");

    fs::write(
        &path,
        format!(
            "{}\n",
            serde_json::to_string_pretty(&snapshot).expect("render snapshot")
        ),
    )
    .expect("write snapshot");

    std::mem::forget(dir);
    path
}

#[test]
fn restore_from_identical_snapshots_yields_identical_committed_state() {
    let snapshot_path = seeded_snapshot_path();
    let first = read_snapshot(&snapshot_path).expect("read first snapshot");
    let second = read_snapshot(&snapshot_path).expect("read second snapshot");

    let first_backend = restore::restore_base_backend(&first).expect("restore first backend");
    let second_backend = restore::restore_base_backend(&second).expect("restore second backend");

    let first_rows = first_backend
        .snapshot_base_table("public.deals")
        .expect("first deals")
        .rows()
        .map(|row| row.values.clone())
        .collect::<Vec<_>>();
    let second_rows = second_backend
        .snapshot_base_table("public.deals")
        .expect("second deals")
        .rows()
        .map(|row| row.values.clone())
        .collect::<Vec<_>>();

    assert_eq!(first_rows, second_rows);
    assert_eq!(
        first_rows,
        vec![
            vec![
                KernelValue::Text(String::from("deal-1")),
                KernelValue::Text(String::from("tenant-1")),
                KernelValue::Text(String::from("Alpha")),
            ],
            vec![
                KernelValue::Text(String::from("deal-2")),
                KernelValue::Text(String::from("tenant-2")),
                KernelValue::Text(String::from("Beta")),
            ],
        ]
    );
}

#[test]
fn reset_semantics_rebuild_a_clean_overlay_from_the_same_snapshot() {
    let snapshot = read_snapshot(&seeded_snapshot_path()).expect("read seeded snapshot");
    let mut overlays = restore::restore_overlay_manager(&snapshot).expect("restore overlays");

    overlays.begin_write("writer").expect("begin writer");
    let mut overlay = overlays
        .snapshot_visible_table("writer", "public.deals")
        .expect("clone committed deals");
    overlay
        .insert_row(vec![
            KernelValue::Text(String::from("deal-3")),
            KernelValue::Text(String::from("tenant-3")),
            KernelValue::Text(String::from("Gamma")),
        ])
        .expect("insert overlay row");
    overlays
        .write_overlay_table("writer", overlay)
        .expect("write overlay");

    assert_eq!(
        overlays
            .visible_table("writer", "public.deals")
            .expect("writer visible deals")
            .row_count(),
        3
    );

    restore::reset_overlay_manager(&snapshot, &mut overlays).expect("reset overlays");

    assert!(overlays.writer_session_id().is_none());
    assert_eq!(
        overlays
            .visible_table("reader", "public.deals")
            .expect("reader visible deals")
            .row_count(),
        2
    );
}
