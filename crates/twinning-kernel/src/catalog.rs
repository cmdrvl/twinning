use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sqlparser::{
    ast::{
        ColumnOption, CreateIndex, CreateTable, Ident, ObjectName, OrderByExpr, Statement,
        TableConstraint,
    },
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Catalog {
    pub dialect: String,
    pub tables: Vec<TableCatalog>,
    pub table_count: usize,
    pub column_count: usize,
    pub index_count: usize,
    pub constraint_count: usize,
}

impl Catalog {
    pub fn table(&self, name: &str) -> Option<&TableCatalog> {
        self.tables.iter().find(|table| table.name == name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableCatalog {
    pub name: String,
    pub columns: Vec<ColumnCatalog>,
    pub primary_key: Option<KeyConstraint>,
    pub unique_constraints: Vec<KeyConstraint>,
    pub foreign_keys: Vec<ForeignKeyConstraint>,
    pub checks: Vec<CheckConstraint>,
    pub indexes: Vec<IndexCatalog>,
}

impl TableCatalog {
    pub fn constraint_count(&self) -> usize {
        usize::from(self.primary_key.is_some())
            + self.unique_constraints.len()
            + self.foreign_keys.len()
            + self.checks.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnCatalog {
    pub name: String,
    pub declared_type: String,
    pub normalized_type: String,
    pub nullable: bool,
    pub default_sql: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyConstraint {
    pub name: Option<String>,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForeignKeyConstraint {
    pub name: Option<String>,
    pub columns: Vec<String>,
    pub foreign_table: String,
    pub referred_columns: Vec<String>,
    pub on_delete: Option<String>,
    pub on_update: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckConstraint {
    pub name: Option<String>,
    pub expression: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexCatalog {
    pub name: Option<String>,
    pub unique: bool,
    pub columns: Vec<String>,
    pub method: Option<String>,
    pub predicate: Option<String>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CatalogError {
    #[error("SQL parse failed: {0}")]
    Parse(String),
    #[error("unsupported schema statement: {0}")]
    UnsupportedStatement(String),
    #[error("duplicate table definition for `{0}`")]
    DuplicateTable(String),
    #[error("index declared for unknown table `{0}`")]
    UnknownIndexTable(String),
    #[error("CREATE TABLE AS / LIKE / CLONE is not supported in bootstrap mode: {0}")]
    UnsupportedCreateTable(String),
}

pub fn parse_postgres_schema(sql: &str) -> Result<Catalog, CatalogError> {
    let dialect = PostgreSqlDialect {};
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|error| CatalogError::Parse(error.to_string()))?;

    let mut tables = BTreeMap::<String, TableCatalog>::new();
    let mut detached_indexes = Vec::<CreateIndex>::new();

    for statement in statements {
        let statement_sql = statement.to_string();
        match statement {
            Statement::CreateTable(create_table) => {
                let table = build_table(create_table, &statement_sql)?;
                if tables.insert(table.name.clone(), table).is_some() {
                    return Err(CatalogError::DuplicateTable(statement_sql));
                }
            }
            Statement::CreateIndex(create_index) => detached_indexes.push(create_index),
            other => {
                if is_ignored_statement(&other.to_string()) {
                    continue;
                }
                return Err(CatalogError::UnsupportedStatement(other.to_string()));
            }
        }
    }

    for create_index in detached_indexes {
        let table_name = object_name_to_string(&create_index.table_name);
        let table = tables
            .get_mut(&table_name)
            .ok_or_else(|| CatalogError::UnknownIndexTable(table_name.clone()))?;
        table.indexes.push(index_from_create_index(create_index));
    }

    let tables = tables.into_values().collect::<Vec<_>>();
    let column_count = tables.iter().map(|table| table.columns.len()).sum();
    let index_count = tables.iter().map(|table| table.indexes.len()).sum();
    let constraint_count = tables.iter().map(TableCatalog::constraint_count).sum();
    let table_count = tables.len();

    Ok(Catalog {
        dialect: "postgres".to_owned(),
        tables,
        table_count,
        column_count,
        index_count,
        constraint_count,
    })
}

fn build_table(
    create_table: CreateTable,
    statement_sql: &str,
) -> Result<TableCatalog, CatalogError> {
    if create_table.query.is_some() || create_table.like.is_some() || create_table.clone.is_some() {
        return Err(CatalogError::UnsupportedCreateTable(
            statement_sql.to_owned(),
        ));
    }

    let mut columns = Vec::with_capacity(create_table.columns.len());
    let mut primary_key = None;
    let mut unique_constraints = Vec::new();
    let mut foreign_keys = Vec::new();
    let mut checks = Vec::new();
    let mut indexes = Vec::new();

    for column in create_table.columns {
        let column_name = ident_to_string(&column.name);
        let mut catalog_column = ColumnCatalog {
            name: column_name.clone(),
            declared_type: column.data_type.to_string(),
            normalized_type: normalize_type(&column.data_type.to_string()),
            nullable: true,
            default_sql: None,
        };

        for option in column.options {
            match option.option {
                ColumnOption::Null => catalog_column.nullable = true,
                ColumnOption::NotNull => catalog_column.nullable = false,
                ColumnOption::Default(expr) => catalog_column.default_sql = Some(expr.to_string()),
                ColumnOption::Unique { is_primary, .. } => {
                    let key = KeyConstraint {
                        name: option.name.map(|name| name.value),
                        columns: vec![column_name.clone()],
                    };
                    if is_primary {
                        primary_key = Some(key);
                        catalog_column.nullable = false;
                    } else {
                        unique_constraints.push(key);
                    }
                }
                ColumnOption::ForeignKey {
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                    ..
                } => foreign_keys.push(ForeignKeyConstraint {
                    name: option.name.map(|name| name.value),
                    columns: vec![column_name.clone()],
                    foreign_table: object_name_to_string(&foreign_table),
                    referred_columns: idents_to_strings(&referred_columns),
                    on_delete: on_delete.map(|action| action.to_string()),
                    on_update: on_update.map(|action| action.to_string()),
                }),
                ColumnOption::Check(expr) => checks.push(CheckConstraint {
                    name: option.name.map(|name| name.value),
                    expression: expr.to_string(),
                }),
                _ => {}
            }
        }

        columns.push(catalog_column);
    }

    for constraint in create_table.constraints {
        match constraint {
            TableConstraint::PrimaryKey { name, columns, .. } => {
                primary_key = Some(KeyConstraint {
                    name: name.map(|name| name.value),
                    columns: idents_to_strings(&columns),
                });
            }
            TableConstraint::Unique { name, columns, .. } => {
                unique_constraints.push(KeyConstraint {
                    name: name.map(|name| name.value),
                    columns: idents_to_strings(&columns),
                })
            }
            TableConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                ..
            } => foreign_keys.push(ForeignKeyConstraint {
                name: name.map(|name| name.value),
                columns: idents_to_strings(&columns),
                foreign_table: object_name_to_string(&foreign_table),
                referred_columns: idents_to_strings(&referred_columns),
                on_delete: on_delete.map(|action| action.to_string()),
                on_update: on_update.map(|action| action.to_string()),
            }),
            TableConstraint::Check { name, expr } => checks.push(CheckConstraint {
                name: name.map(|name| name.value),
                expression: expr.to_string(),
            }),
            TableConstraint::Index {
                name,
                columns,
                index_type,
                ..
            } => indexes.push(IndexCatalog {
                name: name.map(|name| name.value),
                unique: false,
                columns: idents_to_strings(&columns),
                method: index_type.map(|kind| kind.to_string()),
                predicate: None,
            }),
            TableConstraint::FulltextOrSpatial {
                opt_index_name,
                columns,
                ..
            } => indexes.push(IndexCatalog {
                name: opt_index_name.map(|name| name.value),
                unique: false,
                columns: idents_to_strings(&columns),
                method: Some("fulltext_or_spatial".to_owned()),
                predicate: None,
            }),
        }
    }

    Ok(TableCatalog {
        name: object_name_to_string(&create_table.name),
        columns,
        primary_key,
        unique_constraints,
        foreign_keys,
        checks,
        indexes,
    })
}

fn index_from_create_index(create_index: CreateIndex) -> IndexCatalog {
    IndexCatalog {
        name: create_index.name.map(|name| object_name_to_string(&name)),
        unique: create_index.unique,
        columns: create_index
            .columns
            .iter()
            .map(order_by_expr_to_string)
            .collect(),
        method: create_index.using.map(|name| name.value),
        predicate: create_index.predicate.map(|expr| expr.to_string()),
    }
}

fn order_by_expr_to_string(expr: &OrderByExpr) -> String {
    let mut rendered = expr.expr.to_string();
    if let Some(ascending) = expr.asc {
        if ascending {
            rendered.push_str(" ASC");
        } else {
            rendered.push_str(" DESC");
        }
    }
    rendered
}

fn normalize_type(declared_type: &str) -> String {
    let normalized = declared_type.to_ascii_lowercase();
    if normalized.ends_with("[]") {
        return "array".to_owned();
    }
    if normalized.starts_with("bigint") || normalized == "int8" {
        return "bigint".to_owned();
    }
    if normalized.starts_with("int")
        || normalized.starts_with("integer")
        || normalized == "serial"
        || normalized == "int4"
    {
        return "integer".to_owned();
    }
    if normalized.starts_with("smallint") || normalized == "int2" {
        return "smallint".to_owned();
    }
    if normalized.starts_with("numeric") || normalized.starts_with("decimal") {
        return "numeric".to_owned();
    }
    if normalized.starts_with("double")
        || normalized.starts_with("real")
        || normalized.starts_with("float")
    {
        return "float".to_owned();
    }
    if normalized.starts_with("boolean") || normalized == "bool" {
        return "boolean".to_owned();
    }
    if normalized.starts_with("timestamp") {
        return "timestamp".to_owned();
    }
    if normalized == "date" {
        return "date".to_owned();
    }
    if normalized == "bytea" {
        return "bytes".to_owned();
    }
    if normalized.contains("json") {
        return "json".to_owned();
    }
    if normalized.contains("char") || normalized == "text" || normalized == "varchar" {
        return "text".to_owned();
    }
    normalized
}

fn is_ignored_statement(statement_sql: &str) -> bool {
    statement_sql.starts_with("CREATE SCHEMA")
        || statement_sql.starts_with("COMMENT ON")
        || statement_sql.starts_with("SET ")
}

fn object_name_to_string(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(ident_to_string)
        .collect::<Vec<_>>()
        .join(".")
}

fn idents_to_strings(names: &[Ident]) -> Vec<String> {
    names.iter().map(ident_to_string).collect()
}

fn ident_to_string(ident: &Ident) -> String {
    ident.value.clone()
}

#[cfg(test)]
mod tests {
    use super::parse_postgres_schema;

    #[test]
    fn parses_tables_constraints_and_indexes() {
        let sql = r#"
            CREATE SCHEMA IF NOT EXISTS public;

            CREATE TABLE public.sponsors (
                sponsor_id TEXT PRIMARY KEY,
                legal_name TEXT NOT NULL
            );

            CREATE TABLE public.deals (
                deal_id TEXT PRIMARY KEY,
                sponsor_id TEXT REFERENCES public.sponsors (sponsor_id),
                deal_name TEXT NOT NULL UNIQUE,
                dscr NUMERIC CHECK (dscr > 0),
                created_at TIMESTAMP DEFAULT now(),
                CONSTRAINT deals_name_unique UNIQUE (deal_name)
            );

            CREATE UNIQUE INDEX deals_sponsor_idx
                ON public.deals USING btree (sponsor_id, created_at DESC);
        "#;

        let catalog = parse_postgres_schema(sql).expect("schema should parse");
        assert_eq!(catalog.table_count, 2);
        assert_eq!(catalog.column_count, 7);
        assert_eq!(catalog.index_count, 1);

        let deals = catalog
            .table("public.deals")
            .expect("deals table should exist");
        assert_eq!(
            deals.primary_key.as_ref().expect("primary key").columns,
            vec!["deal_id".to_owned()]
        );
        assert_eq!(deals.foreign_keys.len(), 1);
        assert_eq!(deals.checks.len(), 1);
        assert_eq!(deals.unique_constraints.len(), 2);
        assert_eq!(deals.indexes.len(), 1);
        assert_eq!(deals.columns[3].normalized_type, "numeric");
        assert_eq!(deals.columns[4].default_sql.as_deref(), Some("now()"));
    }

    #[test]
    fn refuses_create_table_as_in_bootstrap_mode() {
        let sql = "CREATE TABLE t AS SELECT 1";
        let error = parse_postgres_schema(sql).expect_err("ctas should be refused");
        assert!(error.to_string().contains("CREATE TABLE AS"));
    }
}
