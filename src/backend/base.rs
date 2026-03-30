use std::collections::BTreeMap;

use thiserror::Error;

use crate::kernel::storage::TableStorage;

pub trait Backend {
    fn base_table(&self, table_name: &str) -> Option<&TableStorage>;

    fn overlay_table(&self, table_name: &str) -> Option<&TableStorage>;

    fn write_overlay_table(&mut self, table: TableStorage) -> Result<(), BackendError>;

    fn clear_overlay(&mut self);

    fn visible_table(&self, table_name: &str) -> Option<&TableStorage> {
        self.overlay_table(table_name)
            .or_else(|| self.base_table(table_name))
    }

    fn snapshot_base_table(&self, table_name: &str) -> Result<TableStorage, BackendError> {
        self.base_table(table_name)
            .cloned()
            .ok_or_else(|| BackendError::UnknownTable {
                table: table_name.to_owned(),
            })
    }
}

#[derive(Debug, Clone)]
pub struct BaseSnapshotBackend {
    base_tables: BTreeMap<String, TableStorage>,
    overlay_tables: BTreeMap<String, TableStorage>,
}

impl BaseSnapshotBackend {
    pub fn new(base_tables: impl IntoIterator<Item = TableStorage>) -> Result<Self, BackendError> {
        let mut indexed_tables = BTreeMap::new();

        for table in base_tables {
            let table_name = table.table_name().to_owned();
            if indexed_tables.insert(table_name.clone(), table).is_some() {
                return Err(BackendError::DuplicateBaseTable { table: table_name });
            }
        }

        Ok(Self {
            base_tables: indexed_tables,
            overlay_tables: BTreeMap::new(),
        })
    }

    pub fn table_names(&self) -> Vec<&str> {
        self.base_tables.keys().map(String::as_str).collect()
    }

    pub fn overlay_table_names(&self) -> Vec<&str> {
        self.overlay_tables.keys().map(String::as_str).collect()
    }
}

impl Backend for BaseSnapshotBackend {
    fn base_table(&self, table_name: &str) -> Option<&TableStorage> {
        self.base_tables.get(table_name)
    }

    fn overlay_table(&self, table_name: &str) -> Option<&TableStorage> {
        self.overlay_tables.get(table_name)
    }

    fn write_overlay_table(&mut self, table: TableStorage) -> Result<(), BackendError> {
        let table_name = table.table_name().to_owned();
        if !self.base_tables.contains_key(&table_name) {
            return Err(BackendError::UnknownTable { table: table_name });
        }

        self.overlay_tables.insert(table_name, table);
        Ok(())
    }

    fn clear_overlay(&mut self) {
        self.overlay_tables.clear();
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BackendError {
    #[error("base snapshot already contains table `{table}`")]
    DuplicateBaseTable { table: String },
    #[error("backend has no table named `{table}`")]
    UnknownTable { table: String },
}

#[cfg(test)]
mod tests {
    use crate::{
        backend::base::{Backend, BackendError, BaseSnapshotBackend},
        catalog::parse_postgres_schema,
        kernel::{storage::TableStorage, value::KernelValue},
    };

    fn deals_and_sponsors_storage() -> (TableStorage, TableStorage) {
        let catalog = parse_postgres_schema(
            r#"
            CREATE TABLE public.sponsors (
                sponsor_id TEXT PRIMARY KEY,
                legal_name TEXT NOT NULL
            );

            CREATE TABLE public.deals (
                deal_id TEXT PRIMARY KEY,
                sponsor_id TEXT NOT NULL,
                external_id TEXT UNIQUE,
                deal_name TEXT NOT NULL,
                CONSTRAINT deals_sponsor_fk
                    FOREIGN KEY (sponsor_id) REFERENCES public.sponsors (sponsor_id)
            );
            "#,
        )
        .expect("schema should parse");

        let mut sponsors = TableStorage::new(
            catalog
                .table("public.sponsors")
                .expect("sponsors table should exist"),
        )
        .expect("sponsors storage should build");
        sponsors
            .insert_row(vec![
                KernelValue::Text(String::from("sponsor-1")),
                KernelValue::Text(String::from("Acme Capital")),
            ])
            .expect("insert sponsor");

        let mut deals = TableStorage::new(
            catalog
                .table("public.deals")
                .expect("deals table should exist"),
        )
        .expect("deals storage should build");
        deals
            .insert_row(vec![
                KernelValue::Text(String::from("deal-1")),
                KernelValue::Text(String::from("sponsor-1")),
                KernelValue::Text(String::from("ext-1")),
                KernelValue::Text(String::from("Alpha")),
            ])
            .expect("insert deal");

        (deals, sponsors)
    }

    #[test]
    fn base_snapshot_access_returns_committed_storage_by_table_name() {
        let (deals, sponsors) = deals_and_sponsors_storage();
        let backend =
            BaseSnapshotBackend::new([deals.clone(), sponsors.clone()]).expect("build backend");

        assert_eq!(
            backend.table_names(),
            vec!["public.deals", "public.sponsors"]
        );
        assert_eq!(
            backend
                .base_table("public.deals")
                .expect("deals base table")
                .row_count(),
            1
        );
        assert_eq!(
            backend
                .snapshot_base_table("public.sponsors")
                .expect("clone sponsors table")
                .row_count(),
            sponsors.row_count()
        );
    }

    #[test]
    fn overlay_tables_stay_separate_from_base_snapshot_and_can_be_reset() {
        let (deals, sponsors) = deals_and_sponsors_storage();
        let mut backend =
            BaseSnapshotBackend::new([deals.clone(), sponsors]).expect("build backend");

        let mut overlay_deals = backend
            .snapshot_base_table("public.deals")
            .expect("clone deals for overlay");
        overlay_deals
            .insert_row(vec![
                KernelValue::Text(String::from("deal-2")),
                KernelValue::Text(String::from("sponsor-1")),
                KernelValue::Text(String::from("ext-2")),
                KernelValue::Text(String::from("Beta")),
            ])
            .expect("insert overlay row");
        backend
            .write_overlay_table(overlay_deals)
            .expect("write overlay table");

        assert_eq!(
            backend
                .base_table("public.deals")
                .expect("base deals table")
                .row_count(),
            1
        );
        assert_eq!(
            backend
                .visible_table("public.deals")
                .expect("visible deals table")
                .row_count(),
            2
        );
        assert_eq!(backend.overlay_table_names(), vec!["public.deals"]);

        backend.clear_overlay();

        assert!(backend.overlay_table("public.deals").is_none());
        assert_eq!(
            backend
                .visible_table("public.deals")
                .expect("visible deals after reset")
                .row_count(),
            1
        );
    }

    #[test]
    fn backend_contract_refuses_duplicate_or_unknown_tables_cleanly() {
        let (deals, sponsors) = deals_and_sponsors_storage();
        let duplicate_error =
            BaseSnapshotBackend::new([deals.clone(), deals]).expect_err("duplicate base table");
        assert_eq!(
            duplicate_error,
            BackendError::DuplicateBaseTable {
                table: String::from("public.deals"),
            }
        );

        let mut backend = BaseSnapshotBackend::new([sponsors]).expect("build backend");
        let unknown_table_error = backend
            .snapshot_base_table("public.deals")
            .expect_err("missing table should fail");
        assert_eq!(
            unknown_table_error,
            BackendError::UnknownTable {
                table: String::from("public.deals"),
            }
        );

        let write_unknown_error = backend
            .write_overlay_table(
                BaseSnapshotBackend::new([deals_and_sponsors_storage().0])
                    .expect("temp backend")
                    .snapshot_base_table("public.deals")
                    .expect("temp deals clone"),
            )
            .expect_err("overlay for unknown table should fail");
        assert_eq!(
            write_unknown_error,
            BackendError::UnknownTable {
                table: String::from("public.deals"),
            }
        );
    }
}
