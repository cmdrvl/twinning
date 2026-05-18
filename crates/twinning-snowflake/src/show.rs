//! Snowflake metadata result-set builders for SHOW, DESCRIBE, and CURRENT_* queries.

use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

use crate::catalog::{
    DEFAULT_ROLE_NAME, DEFAULT_USER_NAME, DEFAULT_VERSION, DEFAULT_WAREHOUSE_NAME,
    SnowflakeCatalog, SnowflakeTable, SnowflakeType, normalize_lookup_identifier,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnowflakeResultColumn {
    pub name: String,
    pub sf_type: SnowflakeType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnowflakeResultSet {
    pub columns: Vec<SnowflakeResultColumn>,
    pub rows: Vec<Vec<JsonValue>>,
}

pub fn show_databases(catalog: &SnowflakeCatalog) -> SnowflakeResultSet {
    let columns = text_columns(&[
        "created_on",
        "name",
        "is_default",
        "is_current",
        "origin",
        "owner",
        "comment",
        "options",
        "retention_time",
        "resource_group",
        "drop_on",
        "kind",
    ]);
    let rows = vec![vec![
        JsonValue::Null,
        json!(catalog.database_name),
        json!("Y"),
        json!("Y"),
        json!(""),
        json!(DEFAULT_ROLE_NAME),
        json!(""),
        json!(""),
        json!("1"),
        json!(""),
        JsonValue::Null,
        json!("STANDARD"),
    ]];
    SnowflakeResultSet { columns, rows }
}

pub fn show_schemas(catalog: &SnowflakeCatalog) -> SnowflakeResultSet {
    let columns = text_columns(&[
        "created_on",
        "name",
        "is_default",
        "is_current",
        "database_name",
        "owner",
        "comment",
        "options",
        "retention_time",
        "drop_on",
        "owner_role_type",
    ]);
    let rows = vec![vec![
        JsonValue::Null,
        json!(catalog.schema_name),
        json!("Y"),
        json!("Y"),
        json!(catalog.database_name),
        json!(DEFAULT_ROLE_NAME),
        json!(""),
        json!(""),
        json!("1"),
        JsonValue::Null,
        json!("ROLE"),
    ]];
    SnowflakeResultSet { columns, rows }
}

pub fn show_tables(catalog: &SnowflakeCatalog) -> SnowflakeResultSet {
    let columns = vec![
        result_column("created_on", SnowflakeType::TimestampNtz { scale: 9 }),
        result_column("name", default_text_type()),
        result_column("database_name", default_text_type()),
        result_column("schema_name", default_text_type()),
        result_column("kind", default_text_type()),
        result_column("comment", default_text_type()),
        result_column("cluster_by", default_text_type()),
        result_column(
            "rows",
            SnowflakeType::Fixed {
                precision: 38,
                scale: 0,
            },
        ),
        result_column(
            "bytes",
            SnowflakeType::Fixed {
                precision: 38,
                scale: 0,
            },
        ),
        result_column("owner", default_text_type()),
        result_column("retention_time", default_text_type()),
        result_column("dropped_on", SnowflakeType::TimestampNtz { scale: 9 }),
        result_column("automatic_clustering", default_text_type()),
        result_column("change_tracking", default_text_type()),
        result_column("is_external", default_text_type()),
        result_column("enable_schema_evolution", default_text_type()),
        result_column("owner_role_type", default_text_type()),
        result_column("is_event", default_text_type()),
        result_column("budget", default_text_type()),
        result_column("is_hybrid", default_text_type()),
        result_column("is_iceberg", default_text_type()),
        result_column("is_dynamic", default_text_type()),
        result_column("is_immutable", default_text_type()),
    ];
    let rows = catalog
        .tables
        .iter()
        .map(|table| {
            vec![
                JsonValue::Null,
                json!(table.name),
                json!(catalog.database_name),
                json!(catalog.schema_name),
                json!("TABLE"),
                json!(""),
                json!(""),
                json!(table.rows.len()),
                json!(0),
                json!(DEFAULT_ROLE_NAME),
                json!("1"),
                JsonValue::Null,
                json!("OFF"),
                json!("OFF"),
                json!("N"),
                json!("N"),
                json!("ROLE"),
                json!("N"),
                JsonValue::Null,
                json!("N"),
                json!("N"),
                json!("N"),
                json!("N"),
            ]
        })
        .collect();
    SnowflakeResultSet { columns, rows }
}

pub fn show_columns(catalog: &SnowflakeCatalog, table_name: Option<&str>) -> SnowflakeResultSet {
    let columns = text_columns(&[
        "table_name",
        "schema_name",
        "column_name",
        "data_type",
        "null?",
        "default",
        "kind",
        "expression",
        "comment",
        "database_name",
        "autoincrement",
        "schema_evolution_record",
    ]);
    let tables = matching_tables(catalog, table_name);
    let rows = tables
        .iter()
        .flat_map(|table| {
            table.columns.iter().map(|column| {
                vec![
                    json!(table.name),
                    json!(catalog.schema_name),
                    json!(column.name),
                    json!(column.sf_type.show_data_type_json(column.nullable)),
                    json!(if column.nullable { "Y" } else { "N" }),
                    JsonValue::Null,
                    json!("COLUMN"),
                    JsonValue::Null,
                    json!(""),
                    json!(catalog.database_name),
                    json!(""),
                    JsonValue::Null,
                ]
            })
        })
        .collect();
    SnowflakeResultSet { columns, rows }
}

pub fn show_warehouses() -> SnowflakeResultSet {
    let columns = text_columns(&[
        "name",
        "state",
        "type",
        "size",
        "min_cluster_count",
        "max_cluster_count",
        "started_clusters",
        "running",
        "queued",
        "is_default",
        "is_current",
        "auto_suspend",
        "auto_resume",
        "available",
        "provisioning",
        "quiescing",
        "other",
        "created_on",
        "resumed_on",
        "updated_on",
        "owner",
        "comment",
        "enable_query_acceleration",
        "query_acceleration_max_scale_factor",
        "resource_monitor",
        "actives",
        "pendings",
        "failed",
        "suspended",
        "uuid",
        "scaling_policy",
        "budget",
        "owner_role_type",
    ]);
    let rows = vec![vec![
        json!(DEFAULT_WAREHOUSE_NAME),
        json!("STARTED"),
        json!("STANDARD"),
        json!("X-Small"),
        json!("1"),
        json!("1"),
        json!("1"),
        json!("0"),
        json!("0"),
        json!("Y"),
        json!("Y"),
        json!("600"),
        json!("true"),
        json!("0"),
        json!("0"),
        json!("0"),
        json!("0"),
        JsonValue::Null,
        JsonValue::Null,
        JsonValue::Null,
        json!(DEFAULT_ROLE_NAME),
        json!(""),
        json!("false"),
        json!("0"),
        JsonValue::Null,
        json!("0"),
        json!("0"),
        json!("0"),
        json!("0"),
        json!(""),
        json!("STANDARD"),
        JsonValue::Null,
        json!("ROLE"),
    ]];
    SnowflakeResultSet { columns, rows }
}

pub fn describe_table(catalog: &SnowflakeCatalog, table_name: &str) -> Option<SnowflakeResultSet> {
    let table = catalog.table(table_name)?;
    let columns = text_columns(&[
        "name",
        "type",
        "kind",
        "null?",
        "default",
        "primary key",
        "unique key",
        "check",
        "expression",
        "comment",
        "policy name",
        "privacy domain",
    ]);
    let rows = table
        .columns
        .iter()
        .map(|column| {
            vec![
                json!(column.name),
                json!(column.sf_type.ddl_type()),
                json!("COLUMN"),
                json!(if column.nullable { "Y" } else { "N" }),
                JsonValue::Null,
                json!("N"),
                json!("N"),
                JsonValue::Null,
                JsonValue::Null,
                json!(""),
                JsonValue::Null,
                JsonValue::Null,
            ]
        })
        .collect();
    Some(SnowflakeResultSet { columns, rows })
}

pub fn show_primary_keys() -> SnowflakeResultSet {
    empty_key_result(&[
        "created_on",
        "database_name",
        "schema_name",
        "table_name",
        "column_name",
        "key_sequence",
        "constraint_name",
        "comment",
    ])
}

pub fn show_unique_keys() -> SnowflakeResultSet {
    empty_key_result(&[
        "created_on",
        "database_name",
        "schema_name",
        "table_name",
        "column_name",
        "key_sequence",
        "constraint_name",
        "comment",
    ])
}

pub fn show_imported_keys() -> SnowflakeResultSet {
    empty_key_result(&[
        "created_on",
        "pk_database_name",
        "pk_schema_name",
        "pk_table_name",
        "pk_column_name",
        "fk_database_name",
        "fk_schema_name",
        "fk_table_name",
        "fk_column_name",
        "key_sequence",
        "update_rule",
        "delete_rule",
        "fk_name",
        "pk_name",
        "deferrability",
        "rely",
        "comment",
    ])
}

pub fn current_version_result() -> SnowflakeResultSet {
    scalar_result("CURRENT_VERSION()", json!(DEFAULT_VERSION))
}

pub fn current_database_result(catalog: &SnowflakeCatalog) -> SnowflakeResultSet {
    scalar_result("CURRENT_DATABASE()", json!(catalog.database_name))
}

pub fn current_schema_result(catalog: &SnowflakeCatalog) -> SnowflakeResultSet {
    scalar_result("CURRENT_SCHEMA()", json!(catalog.schema_name))
}

pub fn current_user_result() -> SnowflakeResultSet {
    scalar_result("CURRENT_USER()", json!(DEFAULT_USER_NAME))
}

pub fn current_role_result() -> SnowflakeResultSet {
    scalar_result("CURRENT_ROLE()", json!(DEFAULT_ROLE_NAME))
}

pub fn current_warehouse_result() -> SnowflakeResultSet {
    scalar_result("CURRENT_WAREHOUSE()", json!(DEFAULT_WAREHOUSE_NAME))
}

pub fn metadata_result_for_sql(
    sql: &str,
    catalog: &SnowflakeCatalog,
) -> Option<SnowflakeResultSet> {
    let stripped = strip_sql_comments(sql);
    let trimmed = stripped.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    let normalized_upper = upper.split_whitespace().collect::<Vec<_>>().join(" ");

    if normalized_upper.starts_with("SHOW DATABASES") {
        Some(show_databases(catalog))
    } else if normalized_upper.starts_with("SHOW SCHEMAS") {
        Some(show_schemas(catalog))
    } else if normalized_upper.starts_with("SHOW TABLES")
        || normalized_upper.starts_with("SHOW OBJECTS")
    {
        Some(show_tables(catalog))
    } else if normalized_upper.starts_with("SHOW COLUMNS") {
        let table_name = table_name_after_marker(trimmed, " IN TABLE ")
            .or_else(|| table_name_after_marker(trimmed, " FROM TABLE "));
        Some(show_columns(catalog, table_name.as_deref()))
    } else if normalized_upper.starts_with("SHOW WAREHOUSES") {
        Some(show_warehouses())
    } else if normalized_upper.starts_with("DESCRIBE TABLE ")
        || normalized_upper.starts_with("DESC TABLE ")
    {
        let table_name = trimmed.split_whitespace().last()?;
        describe_table(catalog, table_name)
    } else if normalized_upper.starts_with("SHOW PRIMARY KEYS") {
        Some(show_primary_keys())
    } else if normalized_upper.starts_with("SHOW UNIQUE KEYS") {
        Some(show_unique_keys())
    } else if normalized_upper.starts_with("SHOW IMPORTED KEYS") {
        Some(show_imported_keys())
    } else if normalized_upper == "SELECT CURRENT_VERSION()" {
        Some(current_version_result())
    } else if normalized_upper == "SELECT CURRENT_DATABASE()" {
        Some(current_database_result(catalog))
    } else if normalized_upper == "SELECT CURRENT_SCHEMA()" {
        Some(current_schema_result(catalog))
    } else if normalized_upper == "SELECT CURRENT_USER()" {
        Some(current_user_result())
    } else if normalized_upper == "SELECT CURRENT_ROLE()" {
        Some(current_role_result())
    } else if normalized_upper == "SELECT CURRENT_WAREHOUSE()" {
        Some(current_warehouse_result())
    } else {
        None
    }
}

pub fn strip_sql_comments(sql: &str) -> String {
    let mut output = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '/' && chars.peek() == Some(&'*') {
            chars.next();
            let mut previous = '\0';
            for current in chars.by_ref() {
                if previous == '*' && current == '/' {
                    break;
                }
                previous = current;
            }
            output.push(' ');
        } else if ch == '-' && chars.peek() == Some(&'-') {
            chars.next();
            for current in chars.by_ref() {
                if current == '\n' {
                    output.push('\n');
                    break;
                }
            }
        } else {
            output.push(ch);
        }
    }
    output
}

fn result_column(name: &str, sf_type: SnowflakeType) -> SnowflakeResultColumn {
    SnowflakeResultColumn {
        name: name.to_owned(),
        sf_type,
        nullable: true,
    }
}

fn text_columns(names: &[&str]) -> Vec<SnowflakeResultColumn> {
    names
        .iter()
        .map(|name| result_column(name, default_text_type()))
        .collect()
}

fn default_text_type() -> SnowflakeType {
    SnowflakeType::Text {
        char_length: 16_777_216,
        byte_length: 16_777_216,
    }
}

fn matching_tables<'a>(
    catalog: &'a SnowflakeCatalog,
    table_name: Option<&str>,
) -> Vec<&'a SnowflakeTable> {
    match table_name {
        Some(table_name) => catalog.table(table_name).into_iter().collect(),
        None => catalog.tables.iter().collect(),
    }
}

fn empty_key_result(names: &[&str]) -> SnowflakeResultSet {
    SnowflakeResultSet {
        columns: text_columns(names),
        rows: Vec::new(),
    }
}

fn scalar_result(column_name: &str, value: JsonValue) -> SnowflakeResultSet {
    SnowflakeResultSet {
        columns: vec![result_column(column_name, default_text_type())],
        rows: vec![vec![value]],
    }
}

fn table_name_after_marker(sql: &str, marker: &str) -> Option<String> {
    let upper = sql.to_ascii_uppercase();
    let marker_index = upper.find(marker)?;
    let rest = sql[marker_index + marker.len()..].trim();
    let token = rest.split_whitespace().next()?.trim_end_matches(';');
    Some(normalize_lookup_identifier(token))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{DEFAULT_DATABASE_NAME, DEFAULT_SCHEMA_NAME};

    #[test]
    fn show_tables_returns_exact_column_set() {
        let catalog = SnowflakeCatalog::from_ddl("CREATE TABLE deals (id int);").unwrap();
        let result = show_tables(&catalog);

        assert_eq!(
            result
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec![
                "created_on",
                "name",
                "database_name",
                "schema_name",
                "kind",
                "comment",
                "cluster_by",
                "rows",
                "bytes",
                "owner",
                "retention_time",
                "dropped_on",
                "automatic_clustering",
                "change_tracking",
                "is_external",
                "enable_schema_evolution",
                "owner_role_type",
                "is_event",
                "budget",
                "is_hybrid",
                "is_iceberg",
                "is_dynamic",
                "is_immutable",
            ]
        );
        assert_eq!(result.rows[0][1], json!("DEALS"));
    }

    #[test]
    fn describe_table_returns_snowflake_ddl_type_strings() {
        let catalog = SnowflakeCatalog::from_ddl(
            "CREATE TABLE deals (id int, name varchar, ts timestamp_ntz, active boolean, attrs variant);",
        )
        .unwrap();
        let result = describe_table(&catalog, "deals").unwrap();
        let types = result
            .rows
            .iter()
            .map(|row| row[1].as_str().unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            types,
            vec![
                "NUMBER(38,0)",
                "VARCHAR(16777216)",
                "TIMESTAMP_NTZ(9)",
                "BOOLEAN",
                "VARIANT",
            ]
        );
    }

    #[test]
    fn show_columns_uses_json_string_data_type() {
        let catalog = SnowflakeCatalog::from_ddl("CREATE TABLE t (id int, name varchar);").unwrap();
        let result = show_columns(&catalog, Some("t"));

        assert_eq!(
            result.rows[0][3],
            json!(r#"{"type":"FIXED","precision":38,"scale":0,"nullable":true}"#)
        );
        assert_eq!(
            result.rows[1][3],
            json!(
                r#"{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}"#
            )
        );
    }

    #[test]
    fn strips_jdbc_comment_directives_before_routing() {
        let catalog = SnowflakeCatalog::from_ddl("CREATE TABLE deals (id int);").unwrap();
        let result = metadata_result_for_sql(
            "show /* JDBC:DatabaseMetaData.getTables() */ tables in schema",
            &catalog,
        )
        .unwrap();

        assert_eq!(result.rows[0][1], json!("DEALS"));
    }

    #[test]
    fn current_function_results_are_stubbed() {
        let catalog = SnowflakeCatalog::empty();

        assert_eq!(
            metadata_result_for_sql("select current_database()", &catalog)
                .unwrap()
                .rows[0][0],
            json!(DEFAULT_DATABASE_NAME)
        );
        assert_eq!(
            metadata_result_for_sql("select current_schema()", &catalog)
                .unwrap()
                .rows[0][0],
            json!(DEFAULT_SCHEMA_NAME)
        );
    }
}
