use crate::{
    catalog::Catalog,
    ir::{ColumnName, ScalarValue},
    result::{ReadResult, ResultRow},
};

const DIALECT_VERSION: &str = "PostgreSQL 16.0 (twinning)";
const DEFAULT_SCHEMA: &str = "public";
const TRANSACTION_ISOLATION: &str = "read committed";
const STANDARD_CONFORMING_STRINGS: &str = "on";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MetadataQuery {
    Version,
    CurrentSchema,
    TransactionIsolation,
    StandardConformingStrings,
    InformationSchemaPublicBaseTables,
}

impl MetadataQuery {
    pub(crate) fn columns(self) -> Vec<ColumnName> {
        vec![match self {
            Self::Version => String::from("version"),
            Self::CurrentSchema => String::from("current_schema"),
            Self::TransactionIsolation => String::from("transaction_isolation"),
            Self::StandardConformingStrings => String::from("standard_conforming_strings"),
            Self::InformationSchemaPublicBaseTables => String::from("table_name"),
        }]
    }

    pub(crate) fn result(self, catalog: Option<&Catalog>) -> Option<ReadResult> {
        let rows = match self {
            Self::InformationSchemaPublicBaseTables => public_base_table_rows(catalog?),
            _ => vec![self.row()],
        };

        Some(ReadResult {
            columns: self.columns(),
            rows,
        })
    }

    fn row(self) -> ResultRow {
        let value = match self {
            Self::Version => DIALECT_VERSION,
            Self::CurrentSchema => DEFAULT_SCHEMA,
            Self::TransactionIsolation => TRANSACTION_ISOLATION,
            Self::StandardConformingStrings => STANDARD_CONFORMING_STRINGS,
            Self::InformationSchemaPublicBaseTables => {
                unreachable!("information_schema metadata is catalog-derived")
            }
        };
        vec![ScalarValue::Text(value.to_owned())]
    }
}

fn public_base_table_rows(catalog: &Catalog) -> Vec<ResultRow> {
    catalog
        .tables
        .iter()
        .filter_map(|table| {
            let (schema, table_name) = table
                .name
                .split_once('.')
                .unwrap_or(("public", &table.name));
            (schema == "public").then(|| vec![ScalarValue::Text(table_name.to_owned())])
        })
        .collect()
}

pub(crate) fn classify_metadata_query(sql: &str) -> Option<MetadataQuery> {
    match normalize_sql(sql).as_str() {
        "select pg_catalog.version()" | "select version()" => Some(MetadataQuery::Version),
        "select current_schema()" => Some(MetadataQuery::CurrentSchema),
        "show transaction isolation level" | "show transaction_isolation" => {
            Some(MetadataQuery::TransactionIsolation)
        }
        "show standard_conforming_strings" => Some(MetadataQuery::StandardConformingStrings),
        "select table_name from information_schema.tables where table_schema = 'public' and table_type = 'base table' order by table_name" => {
            Some(MetadataQuery::InformationSchemaPublicBaseTables)
        }
        _ => None,
    }
}

fn normalize_sql(sql: &str) -> String {
    sql.trim()
        .trim_end_matches(';')
        .split_whitespace()
        .map(|token| token.to_ascii_lowercase())
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use crate::{catalog::parse_postgres_schema, ir::ScalarValue};

    use super::{MetadataQuery, classify_metadata_query};

    #[test]
    fn recognizes_declared_metadata_queries_case_insensitively() {
        assert_eq!(
            classify_metadata_query("SELECT pg_catalog.version()"),
            Some(MetadataQuery::Version)
        );
        assert_eq!(
            classify_metadata_query(" select CURRENT_SCHEMA() ; "),
            Some(MetadataQuery::CurrentSchema)
        );
        assert_eq!(
            classify_metadata_query("SHOW transaction isolation level"),
            Some(MetadataQuery::TransactionIsolation)
        );
        assert_eq!(
            classify_metadata_query("show standard_conforming_strings"),
            Some(MetadataQuery::StandardConformingStrings)
        );
        assert_eq!(
            classify_metadata_query(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY table_name",
            ),
            Some(MetadataQuery::InformationSchemaPublicBaseTables)
        );
    }

    #[test]
    fn metadata_results_are_single_text_rows() {
        let version = MetadataQuery::Version
            .result(None)
            .expect("version metadata should not require catalog state");
        assert_eq!(version.columns, vec![String::from("version")]);
        assert_eq!(
            version.rows,
            vec![vec![ScalarValue::Text(String::from(
                "PostgreSQL 16.0 (twinning)"
            ))]]
        );
    }

    #[test]
    fn information_schema_public_base_tables_are_catalog_derived() {
        let catalog = parse_postgres_schema(
            r#"
            CREATE TABLE public.deals (
                deal_id TEXT PRIMARY KEY
            );

            CREATE TABLE public.tenants (
                tenant_id TEXT PRIMARY KEY
            );

            CREATE TABLE scratch.ignored (
                id TEXT PRIMARY KEY
            );
            "#,
        )
        .expect("schema should parse");

        let result = MetadataQuery::InformationSchemaPublicBaseTables
            .result(Some(&catalog))
            .expect("catalog-derived metadata result");

        assert_eq!(result.columns, vec![String::from("table_name")]);
        assert_eq!(
            result.rows,
            vec![
                vec![ScalarValue::Text(String::from("deals"))],
                vec![ScalarValue::Text(String::from("tenants"))],
            ]
        );
        assert!(
            MetadataQuery::InformationSchemaPublicBaseTables
                .result(None)
                .is_none()
        );
    }
}
