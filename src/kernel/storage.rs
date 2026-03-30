use std::collections::BTreeMap;

use thiserror::Error;

use crate::{
    catalog::{KeyConstraint, TableCatalog},
    kernel::value::{KernelValue, ValueType, ValueTypeError},
};

pub type RowId = u64;

#[derive(Debug, Clone, PartialEq)]
pub struct CommittedRow {
    pub id: RowId,
    pub values: Vec<KernelValue>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupSurface {
    pub name: Option<String>,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct TableStorage {
    table_name: String,
    column_types: Vec<ValueType>,
    column_positions: BTreeMap<String, usize>,
    rows: BTreeMap<RowId, CommittedRow>,
    next_row_id: RowId,
    primary_key: Option<KeyIndex>,
    unique_keys: Vec<KeyIndex>,
}

impl TableStorage {
    pub fn new(table: &TableCatalog) -> Result<Self, TableStorageError> {
        let mut column_types = Vec::with_capacity(table.columns.len());
        let mut column_positions = BTreeMap::new();

        for (index, column) in table.columns.iter().enumerate() {
            column_types.push(
                ValueType::from_normalized_catalog_type(&column.normalized_type).map_err(
                    |source| TableStorageError::UnsupportedColumnType {
                        table: table.name.clone(),
                        column: column.name.clone(),
                        normalized_type: column.normalized_type.clone(),
                        source,
                    },
                )?,
            );
            column_positions.insert(column.name.clone(), index);
        }

        let primary_key = table
            .primary_key
            .as_ref()
            .map(|key| KeyIndex::new(&table.name, key, &column_positions))
            .transpose()?;
        let unique_keys = table
            .unique_constraints
            .iter()
            .map(|key| KeyIndex::new(&table.name, key, &column_positions))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            table_name: table.name.clone(),
            column_types,
            column_positions,
            rows: BTreeMap::new(),
            next_row_id: 1,
            primary_key,
            unique_keys,
        })
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn rows(&self) -> impl Iterator<Item = &CommittedRow> {
        self.rows.values()
    }

    pub fn row(&self, row_id: RowId) -> Option<&CommittedRow> {
        self.rows.get(&row_id)
    }

    pub fn primary_key_surface(&self) -> Option<LookupSurface> {
        self.primary_key.as_ref().map(KeyIndex::surface)
    }

    pub fn unique_surfaces(&self) -> Vec<LookupSurface> {
        self.unique_keys.iter().map(KeyIndex::surface).collect()
    }

    pub fn insert_row(&mut self, values: Vec<KernelValue>) -> Result<RowId, TableStorageError> {
        self.validate_row_shape(&values)?;

        let primary_key = self
            .primary_key
            .as_ref()
            .map(|index| {
                index.extract_key(&values).map(|key| {
                    key.ok_or_else(|| TableStorageError::NullPrimaryKey {
                        table: self.table_name.clone(),
                        columns: index.surface.columns.clone(),
                    })
                })
            })
            .transpose()?
            .transpose()?;

        for index in &self.unique_keys {
            if let Some(key) = index.extract_key(&values)?
                && index.rows.contains_key(&key)
            {
                return Err(TableStorageError::DuplicateUniqueKey {
                    table: self.table_name.clone(),
                    columns: index.surface.columns.clone(),
                    name: index.surface.name.clone(),
                });
            }
        }

        if let (Some(index), Some(key)) = (&self.primary_key, primary_key.as_ref())
            && index.rows.contains_key(key)
        {
            return Err(TableStorageError::DuplicatePrimaryKey {
                table: self.table_name.clone(),
                columns: index.surface.columns.clone(),
            });
        }

        let row_id = self.next_row_id;
        self.next_row_id += 1;

        let row = CommittedRow { id: row_id, values };
        self.rows.insert(row_id, row.clone());

        if let (Some(index), Some(key)) = (self.primary_key.as_mut(), primary_key) {
            index.rows.insert(key, row_id);
        }

        for index in &mut self.unique_keys {
            if let Some(key) = index.extract_key(&row.values)? {
                index.rows.insert(key, row_id);
            }
        }

        Ok(row_id)
    }

    pub fn lookup_primary_key(
        &self,
        values: &[KernelValue],
    ) -> Result<Option<&CommittedRow>, TableStorageError> {
        let Some(index) = &self.primary_key else {
            return Ok(None);
        };
        self.lookup(index, values)
    }

    pub fn lookup_unique(
        &self,
        columns: &[&str],
        values: &[KernelValue],
    ) -> Result<Option<&CommittedRow>, TableStorageError> {
        let index = self
            .unique_keys
            .iter()
            .find(|index| index.matches_columns(columns))
            .ok_or_else(|| TableStorageError::UnknownUniqueSurface {
                table: self.table_name.clone(),
                columns: columns.iter().map(|column| (*column).to_owned()).collect(),
            })?;
        self.lookup(index, values)
    }

    fn lookup<'a>(
        &'a self,
        index: &KeyIndex,
        values: &[KernelValue],
    ) -> Result<Option<&'a CommittedRow>, TableStorageError> {
        let Some(key) = index.lookup_key(values)? else {
            return Ok(None);
        };

        Ok(index
            .rows
            .get(&key)
            .and_then(|row_id| self.rows.get(row_id)))
    }

    fn validate_row_shape(&self, values: &[KernelValue]) -> Result<(), TableStorageError> {
        if values.len() != self.column_types.len() {
            return Err(TableStorageError::RowArity {
                table: self.table_name.clone(),
                expected: self.column_types.len(),
                actual: values.len(),
            });
        }

        for (index, value) in values.iter().enumerate() {
            let expected = self.column_types[index];
            if !value.fits_declared_type(expected) {
                return Err(TableStorageError::TypeMismatch {
                    table: self.table_name.clone(),
                    column: self.column_name(index).to_owned(),
                    expected,
                    actual: value.value_type(),
                });
            }
        }

        Ok(())
    }

    fn column_name(&self, index: usize) -> &str {
        self.column_positions
            .iter()
            .find_map(|(name, position)| (*position == index).then_some(name.as_str()))
            .expect("column index should be present")
    }
}

#[derive(Debug, Clone)]
struct KeyIndex {
    surface: LookupSurface,
    positions: Vec<usize>,
    rows: BTreeMap<String, RowId>,
}

impl KeyIndex {
    fn new(
        table_name: &str,
        key: &KeyConstraint,
        column_positions: &BTreeMap<String, usize>,
    ) -> Result<Self, TableStorageError> {
        let positions = key
            .columns
            .iter()
            .map(|column| {
                column_positions.get(column).copied().ok_or_else(|| {
                    TableStorageError::UnknownKeyColumn {
                        table: table_name.to_owned(),
                        column: column.clone(),
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            surface: LookupSurface {
                name: key.name.clone(),
                columns: key.columns.clone(),
            },
            positions,
            rows: BTreeMap::new(),
        })
    }

    fn surface(&self) -> LookupSurface {
        self.surface.clone()
    }

    fn matches_columns(&self, columns: &[&str]) -> bool {
        self.surface.columns.len() == columns.len()
            && self
                .surface
                .columns
                .iter()
                .zip(columns.iter())
                .all(|(left, right)| left == right)
    }

    fn extract_key(&self, values: &[KernelValue]) -> Result<Option<String>, TableStorageError> {
        let key_values = self
            .positions
            .iter()
            .map(|position| values[*position].clone())
            .collect::<Vec<_>>();
        encode_lookup_key(&self.surface.columns, &key_values)
    }

    fn lookup_key(&self, values: &[KernelValue]) -> Result<Option<String>, TableStorageError> {
        if values.len() != self.positions.len() {
            return Err(TableStorageError::LookupArity {
                columns: self.surface.columns.clone(),
                expected: self.positions.len(),
                actual: values.len(),
            });
        }

        encode_lookup_key(&self.surface.columns, values)
    }
}

fn encode_lookup_key(
    columns: &[String],
    values: &[KernelValue],
) -> Result<Option<String>, TableStorageError> {
    if values.iter().any(KernelValue::is_null) {
        return Ok(None);
    }

    serde_json::to_string(values)
        .map(Some)
        .map_err(|source| TableStorageError::LookupEncoding {
            columns: columns.to_vec(),
            source,
        })
}

#[derive(Debug, Error)]
pub enum TableStorageError {
    #[error(
        "table `{table}` column `{column}` normalized type `{normalized_type}` is outside the declared kernel value subset"
    )]
    UnsupportedColumnType {
        table: String,
        column: String,
        normalized_type: String,
        #[source]
        source: ValueTypeError,
    },
    #[error("table `{table}` key column `{column}` is not present in the catalog")]
    UnknownKeyColumn { table: String, column: String },
    #[error("table `{table}` row arity {actual} does not match declared column count {expected}")]
    RowArity {
        table: String,
        expected: usize,
        actual: usize,
    },
    #[error("table `{table}` column `{column}` expected `{expected}` but received `{actual:?}`")]
    TypeMismatch {
        table: String,
        column: String,
        expected: ValueType,
        actual: Option<ValueType>,
    },
    #[error("table `{table}` primary key {columns:?} cannot contain NULL")]
    NullPrimaryKey { table: String, columns: Vec<String> },
    #[error("table `{table}` already has a row for primary key {columns:?}")]
    DuplicatePrimaryKey { table: String, columns: Vec<String> },
    #[error("table `{table}` already has a row for unique key {columns:?}")]
    DuplicateUniqueKey {
        table: String,
        columns: Vec<String>,
        name: Option<String>,
    },
    #[error("table `{table}` has no unique lookup surface for {columns:?}")]
    UnknownUniqueSurface { table: String, columns: Vec<String> },
    #[error("lookup for key {columns:?} expected {expected} values but received {actual}")]
    LookupArity {
        columns: Vec<String>,
        expected: usize,
        actual: usize,
    },
    #[error("lookup key for columns {columns:?} could not be encoded")]
    LookupEncoding {
        columns: Vec<String>,
        #[source]
        source: serde_json::Error,
    },
}

#[cfg(test)]
mod tests {
    use crate::{
        catalog::parse_postgres_schema,
        kernel::{
            storage::{LookupSurface, TableStorage, TableStorageError},
            value::KernelValue,
        },
    };

    fn deals_storage() -> TableStorage {
        let catalog = parse_postgres_schema(
            r#"
            CREATE TABLE public.deals (
                deal_id TEXT PRIMARY KEY,
                external_id TEXT UNIQUE,
                deal_name TEXT NOT NULL,
                tenant_id TEXT,
                CONSTRAINT deals_tenant_name_unique UNIQUE (tenant_id, deal_name)
            );
            "#,
        )
        .expect("schema should parse");

        TableStorage::new(catalog.table("public.deals").expect("deals table"))
            .expect("storage should build")
    }

    #[test]
    fn inserted_rows_keep_stable_identity_and_primary_key_lookup() {
        let mut storage = deals_storage();

        let first_id = storage
            .insert_row(vec![
                KernelValue::Text(String::from("deal-1")),
                KernelValue::Text(String::from("ext-1")),
                KernelValue::Text(String::from("Alpha")),
                KernelValue::Text(String::from("tenant-a")),
            ])
            .expect("insert first row");
        let second_id = storage
            .insert_row(vec![
                KernelValue::Text(String::from("deal-2")),
                KernelValue::Text(String::from("ext-2")),
                KernelValue::Text(String::from("Beta")),
                KernelValue::Text(String::from("tenant-a")),
            ])
            .expect("insert second row");

        assert_eq!(first_id, 1);
        assert_eq!(second_id, 2);
        assert_eq!(storage.row_count(), 2);
        assert_eq!(
            storage.primary_key_surface(),
            Some(LookupSurface {
                name: None,
                columns: vec![String::from("deal_id")],
            })
        );
        assert_eq!(
            storage.row(first_id).expect("row by id").values,
            vec![
                KernelValue::Text(String::from("deal-1")),
                KernelValue::Text(String::from("ext-1")),
                KernelValue::Text(String::from("Alpha")),
                KernelValue::Text(String::from("tenant-a")),
            ]
        );
        assert_eq!(
            storage
                .lookup_primary_key(&[KernelValue::Text(String::from("deal-2"))])
                .expect("lookup primary key")
                .expect("row by primary key")
                .id,
            second_id
        );
    }

    #[test]
    fn unique_lookup_surfaces_return_rows_by_declared_columns() {
        let mut storage = deals_storage();
        let row_id = storage
            .insert_row(vec![
                KernelValue::Text(String::from("deal-3")),
                KernelValue::Text(String::from("ext-3")),
                KernelValue::Text(String::from("Gamma")),
                KernelValue::Text(String::from("tenant-b")),
            ])
            .expect("insert row");

        assert_eq!(
            storage.unique_surfaces(),
            vec![
                LookupSurface {
                    name: None,
                    columns: vec![String::from("external_id")],
                },
                LookupSurface {
                    name: Some(String::from("deals_tenant_name_unique")),
                    columns: vec![String::from("tenant_id"), String::from("deal_name")],
                },
            ]
        );
        assert_eq!(
            storage
                .lookup_unique(
                    &["tenant_id", "deal_name"],
                    &[
                        KernelValue::Text(String::from("tenant-b")),
                        KernelValue::Text(String::from("Gamma")),
                    ],
                )
                .expect("lookup unique surface")
                .expect("row by unique key")
                .id,
            row_id
        );
    }

    #[test]
    fn nullable_unique_columns_do_not_create_lookup_entries() {
        let mut storage = deals_storage();
        storage
            .insert_row(vec![
                KernelValue::Text(String::from("deal-4")),
                KernelValue::Null,
                KernelValue::Text(String::from("Delta")),
                KernelValue::Null,
            ])
            .expect("insert nullable unique row");

        assert!(
            storage
                .lookup_unique(&["external_id"], &[KernelValue::Null])
                .expect("lookup nullable unique column")
                .is_none()
        );
        assert!(
            storage
                .lookup_unique(
                    &["tenant_id", "deal_name"],
                    &[KernelValue::Null, KernelValue::Text(String::from("Delta"))],
                )
                .expect("lookup composite nullable unique column")
                .is_none()
        );
    }

    #[test]
    fn duplicate_key_and_shape_errors_stay_explicit() {
        let mut storage = deals_storage();
        storage
            .insert_row(vec![
                KernelValue::Text(String::from("deal-5")),
                KernelValue::Text(String::from("ext-5")),
                KernelValue::Text(String::from("Epsilon")),
                KernelValue::Text(String::from("tenant-c")),
            ])
            .expect("insert baseline row");

        let duplicate_primary = storage
            .insert_row(vec![
                KernelValue::Text(String::from("deal-5")),
                KernelValue::Text(String::from("ext-6")),
                KernelValue::Text(String::from("Zeta")),
                KernelValue::Text(String::from("tenant-d")),
            ])
            .expect_err("duplicate primary key should fail");
        assert!(matches!(
            duplicate_primary,
            TableStorageError::DuplicatePrimaryKey { .. }
        ));

        let duplicate_unique = storage
            .insert_row(vec![
                KernelValue::Text(String::from("deal-6")),
                KernelValue::Text(String::from("ext-5")),
                KernelValue::Text(String::from("Eta")),
                KernelValue::Text(String::from("tenant-e")),
            ])
            .expect_err("duplicate unique key should fail");
        assert!(matches!(
            duplicate_unique,
            TableStorageError::DuplicateUniqueKey { .. }
        ));

        let wrong_arity = storage
            .insert_row(vec![
                KernelValue::Text(String::from("deal-7")),
                KernelValue::Text(String::from("ext-7")),
            ])
            .expect_err("wrong row arity should fail");
        assert!(matches!(wrong_arity, TableStorageError::RowArity { .. }));

        let wrong_type = storage
            .insert_row(vec![
                KernelValue::Text(String::from("deal-7")),
                KernelValue::Text(String::from("ext-7")),
                KernelValue::Integer(7),
                KernelValue::Text(String::from("tenant-f")),
            ])
            .expect_err("wrong type should fail");
        assert!(matches!(wrong_type, TableStorageError::TypeMismatch { .. }));
    }
}
