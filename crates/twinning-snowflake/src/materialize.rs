//! Source materialization from a real Snowflake account.

use std::{
    collections::BTreeMap,
    env,
    ffi::OsString,
    process::{Command, Output},
};

use serde::Deserialize;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use twinning_kernel::{
    refusal,
    refusal::{RefusalEnvelope, RefusalResult},
};

use crate::catalog::{
    SnowflakeCatalog, SnowflakeSourceMaterialization, SnowflakeTable, SnowflakeType,
    normalize_lookup_identifier,
};

const PYTHON_BIN_ENV: &str = "TWINNING_SNOWFLAKE_PYTHON_BIN";
const MATERIALIZATION_METHOD: &str = "snowflake_connector_fetchall";
const PYTHON_CONNECTOR_IMPORT: &str = "import snowflake.connector";
const PYTHON_FETCH_SCRIPT: &str = r#"
import datetime
import json
import os
import sys

import snowflake.connector

account, user, password, database, schema, table, limit, raw_columns = sys.argv[1:9]
columns = json.loads(raw_columns)

def quote_identifier(value):
    return '"' + value.replace('"', '""') + '"'

def convert(value):
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(datetime.timezone.utc).replace(tzinfo=None)
        return int(value.timestamp() * 1000000)
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return (value - datetime.date(1970, 1, 1)).days
    if isinstance(value, datetime.time):
        return ((value.hour * 3600 + value.minute * 60 + value.second) * 1000000000) + (value.microsecond * 1000)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    return value

kwargs = {
    "account": account,
    "user": user,
    "database": database,
    "schema": schema,
}
if password:
    kwargs["password"] = password
if os.environ.get("SNOWFLAKE_TOKEN"):
    kwargs["token"] = os.environ["SNOWFLAKE_TOKEN"]
if os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"):
    kwargs["private_key_file"] = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]

conn = snowflake.connector.connect(**kwargs)
try:
    cur = conn.cursor()
    select_columns = ", ".join(quote_identifier(column) for column in columns)
    qualified_table = ".".join(quote_identifier(part) for part in [database, schema, table])
    cur.execute(f"SELECT {select_columns} FROM {qualified_table} LIMIT {int(limit)}")
    rows = [[convert(value) for value in row] for row in cur.fetchall()]
    print(json.dumps({"columns": [desc[0] for desc in cur.description], "rows": rows}))
finally:
    conn.close()
"#;

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceUrl {
    account: String,
    database: String,
}

#[derive(Debug, Deserialize)]
struct SourceRows {
    columns: Vec<String>,
    rows: Vec<Vec<JsonValue>>,
}

pub fn apply_source_materialization(
    catalog: &mut SnowflakeCatalog,
    source_url: &str,
    max_rows_per_table: usize,
) -> RefusalResult<()> {
    ensure_python_connector_available()?;
    let source = parse_source_url(source_url)?;
    let account = env::var("SNOWFLAKE_ACCOUNT").unwrap_or(source.account);
    let user = env::var("SNOWFLAKE_USER").unwrap_or_else(|_| "twinning".to_owned());
    let password = env::var("SNOWFLAKE_PASSWORD").unwrap_or_default();
    let schema = catalog.schema_name.clone();
    let mut table_rows = BTreeMap::new();
    let mut rows_by_table = BTreeMap::new();

    for table in &catalog.tables {
        let rows = capture_table(
            &account,
            &user,
            &password,
            &source.database,
            &schema,
            table,
            max_rows_per_table,
        )?;
        table_rows.insert(table.name.clone(), rows.len());
        rows_by_table.insert(table.name.clone(), rows);
    }

    let row_count = table_rows.values().sum();
    let materialization = SnowflakeSourceMaterialization {
        source_identity: format!("sha256:{}", sha256_hex(source_url.as_bytes())),
        method: MATERIALIZATION_METHOD.to_owned(),
        table_count: catalog.tables.len(),
        row_count,
        tables: table_rows,
    };
    catalog
        .replace_rows(rows_by_table, materialization)
        .map_err(|error| Box::new(refusal::runtime_io("snowflake_materialization", error)))
}

fn capture_table(
    account: &str,
    user: &str,
    password: &str,
    database: &str,
    schema: &str,
    table: &SnowflakeTable,
    max_rows_per_table: usize,
) -> RefusalResult<Vec<Vec<JsonValue>>> {
    let columns = table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let raw_columns = serde_json::to_string(&columns)
        .map_err(|error| Box::new(refusal::serialization(error.to_string())))?;
    let output = Command::new(python_bin())
        .arg("-c")
        .arg(PYTHON_FETCH_SCRIPT)
        .arg(account)
        .arg(user)
        .arg(password)
        .arg(database)
        .arg(schema)
        .arg(&table.name)
        .arg(max_rows_per_table.to_string())
        .arg(raw_columns)
        .output()
        .map_err(|error| {
            Box::new(refusal::runtime_io(
                "snowflake_materialization_python",
                error.to_string(),
            ))
        })?;
    let stdout = success_stdout(output, &format!("capture table `{}`", table.name))?;
    let source_rows = serde_json::from_str::<SourceRows>(&stdout).map_err(|error| {
        Box::new(refusal::runtime_io(
            "snowflake_materialization_parse_json",
            format!("{}: {error}; stdout={stdout}", table.name),
        ))
    })?;

    validate_columns(table, &source_rows.columns)?;
    source_rows
        .rows
        .iter()
        .enumerate()
        .map(|(row_index, row)| materialize_row(table, row_index, row))
        .collect()
}

fn validate_columns(table: &SnowflakeTable, source_columns: &[String]) -> RefusalResult<()> {
    let expected = table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let actual = source_columns
        .iter()
        .map(|column| normalize_lookup_identifier(column))
        .collect::<Vec<_>>();
    if actual == expected {
        return Ok(());
    }

    Err(Box::new(refusal::runtime_io(
        "snowflake_materialization_shape",
        format!(
            "{}: source columns [{}] did not match catalog columns [{}]",
            table.name,
            actual.join(", "),
            expected.join(", ")
        ),
    )))
}

fn materialize_row(
    table: &SnowflakeTable,
    row_index: usize,
    row: &[JsonValue],
) -> RefusalResult<Vec<JsonValue>> {
    if row.len() != table.columns.len() {
        return Err(Box::new(refusal::runtime_io(
            "snowflake_materialization_shape",
            format!(
                "{} row {} had {} values but catalog declares {} columns",
                table.name,
                row_index,
                row.len(),
                table.columns.len()
            ),
        )));
    }

    table
        .columns
        .iter()
        .zip(row.iter())
        .map(|(column, value)| {
            coerce_source_value(value, &column.sf_type).map_err(|error| {
                Box::new(refusal::runtime_io(
                    "snowflake_materialization_value",
                    format!("{}.{}: {error}", table.name, column.name),
                ))
            })
        })
        .collect()
}

fn coerce_source_value(value: &JsonValue, sf_type: &SnowflakeType) -> Result<JsonValue, String> {
    if value.is_null() {
        return Ok(JsonValue::Null);
    }

    match sf_type {
        SnowflakeType::Fixed { scale, .. } if *scale == 0 => json_i64(value).map(JsonValue::from),
        SnowflakeType::Fixed { .. } => Ok(JsonValue::String(json_scalar_string(value)?)),
        SnowflakeType::Real => json_f64(value).map(JsonValue::from),
        SnowflakeType::Boolean => json_bool(value).map(JsonValue::from),
        SnowflakeType::Date
        | SnowflakeType::Time { .. }
        | SnowflakeType::TimestampNtz { .. }
        | SnowflakeType::TimestampLtz { .. }
        | SnowflakeType::TimestampTz { .. } => json_i64(value).map(JsonValue::from),
        SnowflakeType::Text { .. }
        | SnowflakeType::Variant
        | SnowflakeType::Object
        | SnowflakeType::Array
        | SnowflakeType::Geography
        | SnowflakeType::Geometry => Ok(JsonValue::String(json_scalar_string(value)?)),
        SnowflakeType::Binary { .. } => value
            .as_str()
            .map(|value| JsonValue::String(value.to_owned()))
            .ok_or_else(|| format!("binary source value `{value}` is not a hex string")),
    }
}

fn json_i64(value: &JsonValue) -> Result<i64, String> {
    if let Some(value) = value.as_i64() {
        return Ok(value);
    }
    if let Some(value) = value.as_u64() {
        return i64::try_from(value).map_err(|_| format!("integer `{value}` exceeds i64"));
    }
    if let Some(value) = value.as_f64() {
        return Ok(value as i64);
    }
    if let Some(value) = value.as_str() {
        return value
            .parse::<i64>()
            .map_err(|error| format!("integer `{value}` did not parse: {error}"));
    }
    Err(format!("value `{value}` is not an integer"))
}

fn json_f64(value: &JsonValue) -> Result<f64, String> {
    if let Some(value) = value.as_f64() {
        return Ok(value);
    }
    if let Some(value) = value.as_str() {
        return value
            .parse::<f64>()
            .map_err(|error| format!("float `{value}` did not parse: {error}"));
    }
    Err(format!("value `{value}` is not a float"))
}

fn json_bool(value: &JsonValue) -> Result<bool, String> {
    if let Some(value) = value.as_bool() {
        return Ok(value);
    }
    if let Some(value) = value.as_str() {
        return Ok(value.eq_ignore_ascii_case("true"));
    }
    Err(format!("value `{value}` is not a boolean"))
}

fn json_scalar_string(value: &JsonValue) -> Result<String, String> {
    match value {
        JsonValue::String(value) => Ok(value.clone()),
        JsonValue::Bool(value) => Ok(value.to_string()),
        JsonValue::Number(value) => Ok(value.to_string()),
        JsonValue::Array(_) | JsonValue::Object(_) => {
            serde_json::to_string(value).map_err(|error| error.to_string())
        }
        JsonValue::Null => Ok(String::new()),
    }
}

fn ensure_python_connector_available() -> RefusalResult<()> {
    let output = Command::new(python_bin())
        .arg("-c")
        .arg(PYTHON_CONNECTOR_IMPORT)
        .output()
        .map_err(|error| {
            Box::new(refusal::runtime_io(
                "snowflake_connector_check",
                error.to_string(),
            ))
        })?;
    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(Box::new(RefusalEnvelope::new(
        "E_SNOWFLAKE_CONNECTOR_REQUIRED",
        "Snowflake source materialization requires the Python snowflake-connector-python package.",
        serde_json::json!({
            "python": python_bin().to_string_lossy(),
            "stderr": stderr.trim(),
        }),
        Some("python3 -m pip install snowflake-connector-python".to_owned()),
    )))
}

fn parse_source_url(source_url: &str) -> RefusalResult<SourceUrl> {
    let Some(rest) = source_url.strip_prefix("snowflake://") else {
        return Err(Box::new(refusal::runtime_io(
            "snowflake_materialization_source_url",
            "source URL must start with snowflake://account/database",
        )));
    };
    let (account, path) = rest.split_once('/').ok_or_else(|| {
        Box::new(refusal::runtime_io(
            "snowflake_materialization_source_url",
            "source URL must include a database path",
        ))
    })?;
    let database = path.split('/').next().unwrap_or_default();
    if account.is_empty() || database.is_empty() {
        return Err(Box::new(refusal::runtime_io(
            "snowflake_materialization_source_url",
            "source URL must include account and database",
        )));
    }

    Ok(SourceUrl {
        account: account.to_owned(),
        database: database.to_owned(),
    })
}

fn success_stdout(output: Output, context: &str) -> RefusalResult<String> {
    let stdout = String::from_utf8(output.stdout).map_err(|error| {
        Box::new(refusal::runtime_io(
            "snowflake_materialization_python",
            format!("{context}: stdout was not UTF-8: {error}"),
        ))
    })?;
    if output.status.success() {
        return Ok(stdout);
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    Err(Box::new(refusal::runtime_io(
        "snowflake_materialization_python",
        format!("{context}: stdout={stdout}; stderr={stderr}"),
    )))
}

fn python_bin() -> OsString {
    env::var_os(PYTHON_BIN_ENV).unwrap_or_else(|| OsString::from("python3"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn coerces_source_values_to_arrow_ready_json_values() {
        assert_eq!(
            coerce_source_value(
                &json!("42"),
                &SnowflakeType::Fixed {
                    precision: 38,
                    scale: 0
                }
            )
            .unwrap(),
            json!(42)
        );
        assert_eq!(
            coerce_source_value(
                &json!(12.34),
                &SnowflakeType::Fixed {
                    precision: 10,
                    scale: 2
                }
            )
            .unwrap(),
            json!("12.34")
        );
        assert_eq!(
            coerce_source_value(&json!({"a": 1}), &SnowflakeType::Variant).unwrap(),
            json!(r#"{"a":1}"#)
        );
    }
}
