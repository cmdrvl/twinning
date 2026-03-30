use crate::{
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
}

impl MetadataQuery {
    pub(crate) fn columns(self) -> Vec<ColumnName> {
        vec![match self {
            Self::Version => String::from("version"),
            Self::CurrentSchema => String::from("current_schema"),
            Self::TransactionIsolation => String::from("transaction_isolation"),
            Self::StandardConformingStrings => String::from("standard_conforming_strings"),
        }]
    }

    pub(crate) fn result(self) -> ReadResult {
        ReadResult {
            columns: self.columns(),
            rows: vec![self.row()],
        }
    }

    fn row(self) -> ResultRow {
        vec![ScalarValue::Text(match self {
            Self::Version => String::from(DIALECT_VERSION),
            Self::CurrentSchema => String::from(DEFAULT_SCHEMA),
            Self::TransactionIsolation => String::from(TRANSACTION_ISOLATION),
            Self::StandardConformingStrings => String::from(STANDARD_CONFORMING_STRINGS),
        })]
    }
}

pub(crate) fn classify_metadata_query(sql: &str) -> Option<MetadataQuery> {
    match normalize_sql(sql).as_str() {
        "select pg_catalog.version()" | "select version()" => Some(MetadataQuery::Version),
        "select current_schema()" => Some(MetadataQuery::CurrentSchema),
        "show transaction isolation level" | "show transaction_isolation" => {
            Some(MetadataQuery::TransactionIsolation)
        }
        "show standard_conforming_strings" => Some(MetadataQuery::StandardConformingStrings),
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
    use crate::ir::ScalarValue;

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
    }

    #[test]
    fn metadata_results_are_single_text_rows() {
        let version = MetadataQuery::Version.result();
        assert_eq!(version.columns, vec![String::from("version")]);
        assert_eq!(
            version.rows,
            vec![vec![ScalarValue::Text(String::from(
                "PostgreSQL 16.0 (twinning)"
            ))]]
        );
    }
}
