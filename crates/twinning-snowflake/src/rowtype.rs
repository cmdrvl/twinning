//! Snowflake ColumnInfo rowtype construction.

use serde_json::{Value as JsonValue, json};

use crate::catalog::SnowflakeColumn;

pub fn column_to_rowtype(column: &SnowflakeColumn) -> JsonValue {
    json!({
        "name": column.name,
        "database": "",
        "schema": "",
        "table": "",
        "nullable": column.nullable,
        "type": column.sf_type.type_string(),
        "byteLength": column.sf_type.byte_length(),
        "length": column.sf_type.char_length(),
        "scale": column.sf_type.scale(),
        "precision": column.sf_type.precision(),
        "collation": JsonValue::Null,
    })
}

pub fn columns_to_rowtype(columns: &[SnowflakeColumn]) -> Vec<JsonValue> {
    columns.iter().map(column_to_rowtype).collect()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::catalog::{SnowflakeCatalog, SnowflakeType};

    use super::*;

    #[test]
    fn builds_rowtype_json_shape() {
        let catalog = SnowflakeCatalog::from_ddl(
            "CREATE TABLE t (id NUMBER(10,2) NOT NULL, name VARCHAR(7));",
        )
        .expect("DDL parses");
        let table = catalog.table("t").unwrap();

        assert_eq!(
            column_to_rowtype(table.column("id").unwrap()),
            json!({
                "name": "ID",
                "database": "",
                "schema": "",
                "table": "",
                "nullable": false,
                "type": "fixed",
                "byteLength": null,
                "length": null,
                "scale": 2,
                "precision": 10,
                "collation": null,
            })
        );
        assert_eq!(
            column_to_rowtype(table.column("name").unwrap()),
            json!({
                "name": "NAME",
                "database": "",
                "schema": "",
                "table": "",
                "nullable": true,
                "type": "text",
                "byteLength": 7,
                "length": 7,
                "scale": null,
                "precision": null,
                "collation": null,
            })
        );
    }

    #[test]
    fn temporal_rowtype_sets_precision_zero() {
        let column = SnowflakeColumn {
            name: "TS".to_owned(),
            sf_type: SnowflakeType::TimestampNtz { scale: 9 },
            nullable: true,
            byte_length: None,
            char_length: None,
            precision: Some(0),
            scale: Some(9),
        };

        assert_eq!(column_to_rowtype(&column)["precision"], json!(0));
        assert_eq!(column_to_rowtype(&column)["scale"], json!(9));
    }
}
