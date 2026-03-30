use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use crate::{
    catalog::{Catalog, TableCatalog},
    cli::Engine,
    kernel::{storage::TableStorage, value::KernelValue},
    refusal,
    refusal::RefusalResult,
    report::VerifyArtifactReport,
};

pub mod restore;

pub const SNAPSHOT_VERSION: &str = "twinning.snapshot.v0";
pub type SnapshotRow = BTreeMap<String, JsonValue>;
pub type SnapshotRelations = BTreeMap<String, Vec<SnapshotRow>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TwinSnapshot {
    pub version: String,
    pub snapshot_hash: String,
    pub created_at: String,
    pub engine: Engine,
    pub mode: String,
    pub schema_source: String,
    pub schema_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_snapshot_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verify_artifact: Option<VerifyArtifactReport>,
    pub catalog: Catalog,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relations: Option<SnapshotRelations>,
    pub table_rows: BTreeMap<String, u64>,
}

impl TwinSnapshot {
    pub fn new(
        engine: Engine,
        schema_source: String,
        schema_hash: String,
        base_snapshot_hash: Option<String>,
        verify_artifact: Option<VerifyArtifactReport>,
        catalog: Catalog,
    ) -> RefusalResult<Self> {
        let table_rows = default_table_rows(&catalog);

        let mut snapshot = Self {
            version: SNAPSHOT_VERSION.to_owned(),
            snapshot_hash: String::new(),
            created_at: Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
            engine,
            mode: "catalog_only".to_owned(),
            schema_source,
            schema_hash,
            base_snapshot_hash,
            verify_artifact,
            catalog,
            relations: None,
            table_rows,
        };
        snapshot.snapshot_hash = snapshot.compute_hash()?;
        Ok(snapshot)
    }

    pub fn with_relations(mut self, relations: SnapshotRelations) -> RefusalResult<Self> {
        let relations = canonicalize_relations(&self.catalog, &relations)?;
        self.table_rows = table_rows_for_relations(&self.catalog, Some(&relations));
        self.relations = Some(relations);
        self.snapshot_hash = self.compute_hash()?;
        Ok(self)
    }

    pub fn with_committed_tables(
        self,
        committed_tables: impl IntoIterator<Item = TableStorage>,
    ) -> RefusalResult<Self> {
        let relations = relations_from_committed_tables(&self.catalog, committed_tables)?;
        self.with_relations(relations)
    }

    pub fn compute_hash(&self) -> RefusalResult<String> {
        let mut clone = self.clone();
        clone.snapshot_hash.clear();
        clone.created_at.clear();
        if let Some(relations) = clone.relations.clone() {
            let relations = canonicalize_relations(&clone.catalog, &relations)?;
            clone.table_rows = table_rows_for_relations(&clone.catalog, Some(&relations));
            clone.relations = Some(relations);
        } else {
            clone.table_rows = table_rows_for_relations(&clone.catalog, None);
        }
        let bytes = serde_json::to_vec(&clone)
            .map_err(|error| Box::new(refusal::serialization(error.to_string())))?;
        let mut digest = Sha256::new();
        digest.update(bytes);
        Ok(format!("sha256:{:x}", digest.finalize()))
    }
}

fn default_table_rows(catalog: &Catalog) -> BTreeMap<String, u64> {
    catalog
        .tables
        .iter()
        .map(|table| (table.name.clone(), 0))
        .collect()
}

fn table_rows_for_relations(
    catalog: &Catalog,
    relations: Option<&SnapshotRelations>,
) -> BTreeMap<String, u64> {
    let mut table_rows = default_table_rows(catalog);
    if let Some(relations) = relations {
        for (table_name, rows) in relations {
            table_rows.insert(table_name.clone(), rows.len() as u64);
        }
    }
    table_rows
}

fn canonicalize_relations(
    catalog: &Catalog,
    relations: &SnapshotRelations,
) -> RefusalResult<SnapshotRelations> {
    let expected_tables = catalog
        .tables
        .iter()
        .map(|table| table.name.clone())
        .collect::<BTreeSet<_>>();
    let actual_tables = relations.keys().cloned().collect::<BTreeSet<_>>();

    if expected_tables != actual_tables {
        let missing = expected_tables
            .difference(&actual_tables)
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        let unexpected = actual_tables
            .difference(&expected_tables)
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        return Err(Box::new(refusal::serialization(format!(
            "snapshot relations must cover every catalog table exactly once; missing [{missing}], unexpected [{unexpected}]"
        ))));
    }

    let mut canonical = BTreeMap::new();
    for table in &catalog.tables {
        let rows = relations
            .get(&table.name)
            .expect("validated relation coverage");
        canonical.insert(table.name.clone(), canonicalize_table_rows(table, rows)?);
    }

    Ok(canonical)
}

pub(crate) fn relations_from_committed_tables(
    catalog: &Catalog,
    committed_tables: impl IntoIterator<Item = TableStorage>,
) -> RefusalResult<SnapshotRelations> {
    let mut storage_by_name = BTreeMap::new();
    for storage in committed_tables {
        let table_name = storage.table_name().to_owned();
        if storage_by_name
            .insert(table_name.clone(), storage)
            .is_some()
        {
            return Err(Box::new(refusal::serialization(format!(
                "committed snapshot state includes duplicate table `{table_name}`"
            ))));
        }
    }

    let mut relations = BTreeMap::new();
    for table in &catalog.tables {
        let rows = storage_by_name
            .remove(&table.name)
            .map(|storage| snapshot_rows_from_storage(table, &storage))
            .transpose()?
            .unwrap_or_default();
        relations.insert(table.name.clone(), rows);
    }

    if let Some(unexpected_table) = storage_by_name.keys().next() {
        return Err(Box::new(refusal::serialization(format!(
            "committed snapshot state includes undeclared table `{unexpected_table}`"
        ))));
    }

    Ok(relations)
}

fn snapshot_rows_from_storage(
    table: &TableCatalog,
    storage: &TableStorage,
) -> RefusalResult<Vec<SnapshotRow>> {
    if storage.table_name() != table.name {
        return Err(Box::new(refusal::serialization(format!(
            "committed storage table `{}` does not match declared table `{}`",
            storage.table_name(),
            table.name
        ))));
    }

    let rows = storage
        .rows()
        .map(|row| snapshot_row_from_values(table, &row.values))
        .collect::<RefusalResult<Vec<_>>>()?;

    canonicalize_table_rows(table, &rows)
}

fn snapshot_row_from_values(
    table: &TableCatalog,
    values: &[KernelValue],
) -> RefusalResult<SnapshotRow> {
    if values.len() != table.columns.len() {
        return Err(Box::new(refusal::serialization(format!(
            "committed storage row for `{}` has {} values but catalog declares {} columns",
            table.name,
            values.len(),
            table.columns.len()
        ))));
    }

    table
        .columns
        .iter()
        .zip(values.iter())
        .map(|(column, value)| {
            let value = serde_json::to_value(value)
                .map_err(|error| Box::new(refusal::serialization(error.to_string())))?;
            Ok((column.name.clone(), value))
        })
        .collect::<RefusalResult<BTreeMap<_, _>>>()
}

fn canonicalize_table_rows(
    table: &TableCatalog,
    rows: &[SnapshotRow],
) -> RefusalResult<Vec<SnapshotRow>> {
    let expected_columns = table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<BTreeSet<_>>();
    let declared_order = table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let primary_key_columns = table
        .primary_key
        .as_ref()
        .map(|key| key.columns.clone())
        .unwrap_or_else(|| declared_order.clone());

    let mut keyed_rows = rows
        .iter()
        .cloned()
        .map(|row| {
            let actual_columns = row.keys().cloned().collect::<BTreeSet<_>>();
            if actual_columns != expected_columns {
                let missing = expected_columns
                    .difference(&actual_columns)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ");
                let unexpected = actual_columns
                    .difference(&expected_columns)
                    .cloned()
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(Box::new(refusal::serialization(format!(
                    "snapshot relation `{}` row columns must match declared catalog columns exactly; missing [{missing}], unexpected [{unexpected}]",
                    table.name
                ))));
            }

            Ok((
                snapshot_row_key(&row, &primary_key_columns)?,
                snapshot_row_key(&row, &declared_order)?,
                row,
            ))
        })
        .collect::<RefusalResult<Vec<_>>>()?;

    keyed_rows.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    Ok(keyed_rows
        .into_iter()
        .map(|(_, _, row)| row)
        .collect::<Vec<_>>())
}

fn snapshot_row_key(row: &SnapshotRow, columns: &[String]) -> RefusalResult<String> {
    let mut encoded = Vec::with_capacity(columns.len());
    for column in columns {
        let value = row.get(column).ok_or_else(|| {
            Box::new(refusal::serialization(format!(
                "snapshot row is missing declared column `{column}`"
            )))
        })?;
        encoded.push(
            serde_json::to_string(value)
                .map_err(|error| Box::new(refusal::serialization(error.to_string())))?,
        );
    }

    Ok(encoded.join("\u{1f}"))
}

pub fn write_snapshot(path: &Path, snapshot: &TwinSnapshot) -> RefusalResult<String> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .map_err(|error| Box::new(refusal::io_write(path, &error)))?;
    }

    let rendered = serde_json::to_string_pretty(snapshot)
        .map_err(|error| Box::new(refusal::serialization(error.to_string())))?;
    std::fs::write(path, format!("{rendered}\n"))
        .map_err(|error| Box::new(refusal::io_write(path, &error)))?;
    Ok(snapshot.snapshot_hash.clone())
}

pub fn read_snapshot(path: &Path) -> RefusalResult<TwinSnapshot> {
    let raw =
        std::fs::read_to_string(path).map_err(|error| Box::new(refusal::io_read(path, &error)))?;
    let snapshot: TwinSnapshot = serde_json::from_str(&raw)
        .map_err(|error| Box::new(refusal::snapshot_verify(path, error.to_string())))?;

    if snapshot.version != SNAPSHOT_VERSION {
        return Err(Box::new(refusal::snapshot_verify(
            path,
            format!(
                "unsupported snapshot version `{}` (expected `{SNAPSHOT_VERSION}`)",
                snapshot.version
            ),
        )));
    }

    let computed_hash = snapshot.compute_hash()?;
    if computed_hash != snapshot.snapshot_hash {
        return Err(Box::new(refusal::snapshot_verify(
            path,
            format!(
                "snapshot hash mismatch: expected {}, computed {}",
                snapshot.snapshot_hash, computed_hash
            ),
        )));
    }

    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tempfile::tempdir;

    use crate::{
        catalog::{Catalog, TableCatalog},
        cli::Engine,
        kernel::{storage::TableStorage, value::KernelValue},
    };

    use super::{JsonValue, SnapshotRelations, TwinSnapshot, read_snapshot, write_snapshot};

    #[test]
    fn snapshot_round_trips_and_verifies_hash() {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("bootstrap.twin");
        let catalog = deals_catalog();

        let snapshot = TwinSnapshot::new(
            Engine::Postgres,
            "schema.sql".to_owned(),
            "sha256:abc".to_owned(),
            None,
            None,
            catalog,
        )
        .expect("snapshot");

        let hash = write_snapshot(&path, &snapshot).expect("write snapshot");
        let restored = read_snapshot(&path).expect("read snapshot");

        assert_eq!(hash, restored.snapshot_hash);
        assert_eq!(
            restored.table_rows,
            BTreeMap::from([(String::from("public.deals"), 0)])
        );
    }

    #[test]
    fn snapshot_hash_ignores_created_at_but_includes_base_snapshot_hash() {
        let catalog = Catalog {
            dialect: "postgres".to_owned(),
            tables: Vec::new(),
            table_count: 0,
            column_count: 0,
            index_count: 0,
            constraint_count: 0,
        };

        let snapshot = TwinSnapshot::new(
            Engine::Postgres,
            "schema.sql".to_owned(),
            "sha256:abc".to_owned(),
            Some("sha256:parent".to_owned()),
            None,
            catalog,
        )
        .expect("snapshot");

        let mut changed_timestamp = snapshot.clone();
        changed_timestamp.created_at = "2030-01-01T00:00:00Z".to_owned();
        assert_eq!(
            snapshot.compute_hash().expect("hash"),
            changed_timestamp.compute_hash().expect("hash")
        );

        let mut changed_parent = snapshot;
        changed_parent.base_snapshot_hash = Some("sha256:other".to_owned());
        assert_ne!(
            changed_parent.compute_hash().expect("hash"),
            changed_timestamp.compute_hash().expect("hash")
        );
    }

    #[test]
    fn snapshot_relations_are_canonicalized_by_primary_key_and_update_row_counts() {
        let catalog = deals_catalog();
        let snapshot = TwinSnapshot::new(
            Engine::Postgres,
            "schema.sql".to_owned(),
            "sha256:abc".to_owned(),
            None,
            None,
            catalog,
        )
        .expect("snapshot")
        .with_relations(deals_relations())
        .expect("relations");

        assert_eq!(
            snapshot.table_rows,
            BTreeMap::from([(String::from("public.deals"), 2)])
        );
        assert_eq!(
            snapshot.relations.expect("relations")["public.deals"],
            vec![
                BTreeMap::from([
                    (
                        String::from("deal_id"),
                        JsonValue::String(String::from("deal-1"))
                    ),
                    (
                        String::from("deal_name"),
                        JsonValue::String(String::from("Alpha"))
                    ),
                ]),
                BTreeMap::from([
                    (
                        String::from("deal_id"),
                        JsonValue::String(String::from("deal-2"))
                    ),
                    (
                        String::from("deal_name"),
                        JsonValue::String(String::from("Beta"))
                    ),
                ]),
            ]
        );
    }

    #[test]
    fn snapshot_hash_is_stable_across_relation_input_order() {
        let catalog = deals_catalog();
        let baseline = TwinSnapshot::new(
            Engine::Postgres,
            "schema.sql".to_owned(),
            "sha256:abc".to_owned(),
            None,
            None,
            catalog.clone(),
        )
        .expect("snapshot")
        .with_relations(deals_relations())
        .expect("relations");

        let mut reordered = TwinSnapshot::new(
            Engine::Postgres,
            "schema.sql".to_owned(),
            "sha256:abc".to_owned(),
            None,
            None,
            catalog,
        )
        .expect("snapshot")
        .with_relations(BTreeMap::from([(
            String::from("public.deals"),
            vec![
                BTreeMap::from([
                    (
                        String::from("deal_name"),
                        JsonValue::String(String::from("Beta")),
                    ),
                    (
                        String::from("deal_id"),
                        JsonValue::String(String::from("deal-2")),
                    ),
                ]),
                BTreeMap::from([
                    (
                        String::from("deal_name"),
                        JsonValue::String(String::from("Alpha")),
                    ),
                    (
                        String::from("deal_id"),
                        JsonValue::String(String::from("deal-1")),
                    ),
                ]),
            ],
        )]))
        .expect("relations");
        reordered.created_at = "2036-03-30T00:00:00Z".to_owned();

        assert_eq!(
            baseline.compute_hash().expect("baseline hash"),
            reordered.compute_hash().expect("reordered hash")
        );
    }

    #[test]
    fn snapshot_relations_refuse_missing_catalog_columns() {
        let snapshot = TwinSnapshot::new(
            Engine::Postgres,
            "schema.sql".to_owned(),
            "sha256:abc".to_owned(),
            None,
            None,
            deals_catalog(),
        )
        .expect("snapshot");

        let error = snapshot
            .with_relations(BTreeMap::from([(
                String::from("public.deals"),
                vec![BTreeMap::from([(
                    String::from("deal_id"),
                    JsonValue::String(String::from("deal-1")),
                )])],
            )]))
            .expect_err("missing columns should fail");

        assert!(
            error
                .render(true)
                .expect("render refusal")
                .contains("row columns must match declared catalog columns exactly")
        );
    }

    #[test]
    fn committed_tables_are_canonicalized_by_primary_key_and_update_row_counts() {
        let catalog = deals_catalog();
        let deals_table = catalog.table("public.deals").expect("deals table");
        let mut storage = TableStorage::new(deals_table).expect("deals storage should build");
        storage
            .insert_row(vec![
                KernelValue::Text(String::from("deal-2")),
                KernelValue::Text(String::from("Beta")),
            ])
            .expect("insert second row first");
        storage
            .insert_row(vec![
                KernelValue::Text(String::from("deal-1")),
                KernelValue::Text(String::from("Alpha")),
            ])
            .expect("insert first row second");

        let snapshot = TwinSnapshot::new(
            Engine::Postgres,
            "schema.sql".to_owned(),
            "sha256:abc".to_owned(),
            None,
            None,
            catalog,
        )
        .expect("snapshot")
        .with_committed_tables([storage])
        .expect("committed relations");

        assert_eq!(
            snapshot.table_rows,
            BTreeMap::from([(String::from("public.deals"), 2)])
        );
        assert_eq!(
            snapshot.relations.expect("relations")["public.deals"],
            vec![
                BTreeMap::from([
                    (
                        String::from("deal_id"),
                        json_kernel_value(KernelValue::Text(String::from("deal-1"))),
                    ),
                    (
                        String::from("deal_name"),
                        json_kernel_value(KernelValue::Text(String::from("Alpha"))),
                    ),
                ]),
                BTreeMap::from([
                    (
                        String::from("deal_id"),
                        json_kernel_value(KernelValue::Text(String::from("deal-2"))),
                    ),
                    (
                        String::from("deal_name"),
                        json_kernel_value(KernelValue::Text(String::from("Beta"))),
                    ),
                ]),
            ]
        );
    }

    #[test]
    fn committed_tables_without_primary_key_sort_by_full_row_tuple() {
        let catalog = events_catalog();
        let events_table = catalog.table("public.events").expect("events table");
        let mut storage = TableStorage::new(events_table).expect("events storage should build");
        storage
            .insert_row(vec![
                KernelValue::Text(String::from("b")),
                KernelValue::Integer(2),
            ])
            .expect("insert b row");
        storage
            .insert_row(vec![
                KernelValue::Text(String::from("a")),
                KernelValue::Integer(3),
            ])
            .expect("insert a3 row");
        storage
            .insert_row(vec![
                KernelValue::Text(String::from("a")),
                KernelValue::Integer(1),
            ])
            .expect("insert a1 row");

        let snapshot = TwinSnapshot::new(
            Engine::Postgres,
            "schema.sql".to_owned(),
            "sha256:abc".to_owned(),
            None,
            None,
            catalog,
        )
        .expect("snapshot")
        .with_committed_tables([storage])
        .expect("committed relations");

        assert_eq!(
            snapshot.relations.expect("relations")["public.events"],
            vec![
                BTreeMap::from([
                    (
                        String::from("event_name"),
                        json_kernel_value(KernelValue::Text(String::from("a"))),
                    ),
                    (
                        String::from("sequence"),
                        json_kernel_value(KernelValue::Integer(1)),
                    ),
                ]),
                BTreeMap::from([
                    (
                        String::from("event_name"),
                        json_kernel_value(KernelValue::Text(String::from("a"))),
                    ),
                    (
                        String::from("sequence"),
                        json_kernel_value(KernelValue::Integer(3)),
                    ),
                ]),
                BTreeMap::from([
                    (
                        String::from("event_name"),
                        json_kernel_value(KernelValue::Text(String::from("b"))),
                    ),
                    (
                        String::from("sequence"),
                        json_kernel_value(KernelValue::Integer(2)),
                    ),
                ]),
            ]
        );
    }

    fn deals_catalog() -> Catalog {
        Catalog {
            dialect: "postgres".to_owned(),
            tables: vec![TableCatalog {
                name: "public.deals".to_owned(),
                columns: vec![
                    crate::catalog::ColumnCatalog {
                        name: "deal_id".to_owned(),
                        declared_type: "TEXT".to_owned(),
                        normalized_type: "text".to_owned(),
                        nullable: false,
                        default_sql: None,
                    },
                    crate::catalog::ColumnCatalog {
                        name: "deal_name".to_owned(),
                        declared_type: "TEXT".to_owned(),
                        normalized_type: "text".to_owned(),
                        nullable: false,
                        default_sql: None,
                    },
                ],
                primary_key: Some(crate::catalog::KeyConstraint {
                    name: None,
                    columns: vec![String::from("deal_id")],
                }),
                unique_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                checks: Vec::new(),
                indexes: Vec::new(),
            }],
            table_count: 1,
            column_count: 2,
            index_count: 0,
            constraint_count: 1,
        }
    }

    fn deals_relations() -> SnapshotRelations {
        BTreeMap::from([(
            String::from("public.deals"),
            vec![
                BTreeMap::from([
                    (
                        String::from("deal_id"),
                        JsonValue::String(String::from("deal-2")),
                    ),
                    (
                        String::from("deal_name"),
                        JsonValue::String(String::from("Beta")),
                    ),
                ]),
                BTreeMap::from([
                    (
                        String::from("deal_id"),
                        JsonValue::String(String::from("deal-1")),
                    ),
                    (
                        String::from("deal_name"),
                        JsonValue::String(String::from("Alpha")),
                    ),
                ]),
            ],
        )])
    }

    fn events_catalog() -> Catalog {
        Catalog {
            dialect: "postgres".to_owned(),
            tables: vec![TableCatalog {
                name: "public.events".to_owned(),
                columns: vec![
                    crate::catalog::ColumnCatalog {
                        name: "event_name".to_owned(),
                        declared_type: "TEXT".to_owned(),
                        normalized_type: "text".to_owned(),
                        nullable: true,
                        default_sql: None,
                    },
                    crate::catalog::ColumnCatalog {
                        name: "sequence".to_owned(),
                        declared_type: "INTEGER".to_owned(),
                        normalized_type: "integer".to_owned(),
                        nullable: true,
                        default_sql: None,
                    },
                ],
                primary_key: None,
                unique_constraints: Vec::new(),
                foreign_keys: Vec::new(),
                checks: Vec::new(),
                indexes: Vec::new(),
            }],
            table_count: 1,
            column_count: 2,
            index_count: 0,
            constraint_count: 0,
        }
    }

    fn json_kernel_value(value: KernelValue) -> JsonValue {
        serde_json::to_value(value).expect("serialize kernel value")
    }
}
