use crate::{
    backend::{BaseSnapshotBackend, SessionOverlayManager},
    kernel::{storage::TableStorage, value::KernelValue},
    refusal,
    refusal::RefusalResult,
    snapshot::{SnapshotRelations, TwinSnapshot},
};

pub fn restore_committed_tables(snapshot: &TwinSnapshot) -> RefusalResult<Vec<TableStorage>> {
    let relations = canonical_snapshot_relations(snapshot)?;
    let mut restored_tables = Vec::with_capacity(snapshot.catalog.tables.len());

    for table in &snapshot.catalog.tables {
        let mut storage = TableStorage::new(table)
            .map_err(|error| Box::new(refusal::serialization(error.to_string())))?;

        if let Some(rows) = relations
            .as_ref()
            .and_then(|relations| relations.get(&table.name))
        {
            for row in rows {
                let values = table
                    .columns
                    .iter()
                    .map(|column| {
                        let value = row.get(&column.name).cloned().ok_or_else(|| {
                            Box::new(refusal::serialization(format!(
                                "snapshot relation `{}` row is missing declared column `{}`",
                                table.name, column.name
                            )))
                        })?;
                        serde_json::from_value::<KernelValue>(value)
                            .map_err(|error| Box::new(refusal::serialization(error.to_string())))
                    })
                    .collect::<RefusalResult<Vec<_>>>()?;

                storage.insert_row(values).map_err(|error| {
                    Box::new(refusal::serialization(format!(
                        "failed to restore committed row into `{}`: {error}",
                        table.name
                    )))
                })?;
            }
        }

        restored_tables.push(storage);
    }

    Ok(restored_tables)
}

pub fn restore_base_backend(snapshot: &TwinSnapshot) -> RefusalResult<BaseSnapshotBackend> {
    BaseSnapshotBackend::new(restore_committed_tables(snapshot)?)
        .map_err(|error| Box::new(refusal::serialization(error.to_string())))
}

pub fn restore_overlay_manager(snapshot: &TwinSnapshot) -> RefusalResult<SessionOverlayManager> {
    Ok(SessionOverlayManager::new(restore_base_backend(snapshot)?))
}

pub fn reset_overlay_manager(
    snapshot: &TwinSnapshot,
    overlays: &mut SessionOverlayManager,
) -> RefusalResult<()> {
    *overlays = restore_overlay_manager(snapshot)?;
    Ok(())
}

fn canonical_snapshot_relations(
    snapshot: &TwinSnapshot,
) -> RefusalResult<Option<SnapshotRelations>> {
    snapshot
        .relations
        .as_ref()
        .map(|relations| super::canonicalize_relations(&snapshot.catalog, relations))
        .transpose()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use crate::{
        backend::Backend, catalog::parse_postgres_schema, cli::Engine, kernel::value::KernelValue,
        snapshot::TwinSnapshot,
    };

    use super::{reset_overlay_manager, restore_base_backend, restore_overlay_manager};

    #[test]
    fn restore_base_backend_rehydrates_canonical_committed_rows() {
        let snapshot = seeded_snapshot();

        let backend = restore_base_backend(&snapshot).expect("restore backend");
        let restored = backend
            .snapshot_base_table("public.deals")
            .expect("restore deals table");
        let restored_rows = restored.rows().collect::<Vec<_>>();

        assert_eq!(restored_rows.len(), 2);
        assert_eq!(
            restored_rows[0].values,
            vec![
                KernelValue::Text(String::from("deal-1")),
                KernelValue::Text(String::from("tenant-1")),
                KernelValue::Text(String::from("Alpha")),
            ]
        );
        assert_eq!(
            restored_rows[1].values,
            vec![
                KernelValue::Text(String::from("deal-2")),
                KernelValue::Text(String::from("tenant-2")),
                KernelValue::Text(String::from("Beta")),
            ]
        );
    }

    #[test]
    fn reset_overlay_manager_discards_uncommitted_overlay_state() {
        let snapshot = seeded_snapshot();
        let mut overlays = restore_overlay_manager(&snapshot).expect("restore overlays");

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
        assert_eq!(overlays.writer_session_id(), Some("writer"));

        reset_overlay_manager(&snapshot, &mut overlays).expect("reset overlays");

        assert!(overlays.writer_session_id().is_none());
        assert_eq!(
            overlays
                .visible_table("writer", "public.deals")
                .expect("restored writer view")
                .row_count(),
            2
        );
        assert_eq!(
            overlays
                .visible_table("reader", "public.deals")
                .expect("restored reader view")
                .row_count(),
            2
        );
    }

    fn seeded_snapshot() -> TwinSnapshot {
        TwinSnapshot::new(
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
        .with_relations(BTreeMap::from([(
            String::from("public.deals"),
            vec![
                BTreeMap::from([
                    (
                        String::from("deal_id"),
                        json!({ "kind": "text", "value": "deal-2" }),
                    ),
                    (
                        String::from("tenant_id"),
                        json!({ "kind": "text", "value": "tenant-2" }),
                    ),
                    (
                        String::from("deal_name"),
                        json!({ "kind": "text", "value": "Beta" }),
                    ),
                ]),
                BTreeMap::from([
                    (
                        String::from("deal_id"),
                        json!({ "kind": "text", "value": "deal-1" }),
                    ),
                    (
                        String::from("tenant_id"),
                        json!({ "kind": "text", "value": "tenant-1" }),
                    ),
                    (
                        String::from("deal_name"),
                        json!({ "kind": "text", "value": "Alpha" }),
                    ),
                ]),
            ],
        )]))
        .expect("seed relations")
    }
}
