use std::{
    collections::BTreeMap,
    ffi::OsString,
    process::{Command, Output},
};

use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};

use crate::{
    catalog::{Catalog, TableCatalog},
    kernel::value::{KernelValue, ValueType},
    refusal,
    refusal::RefusalResult,
    report::SourceMaterializationReport,
    snapshot::{SnapshotRelations, SnapshotRow},
};

const PSQL_BIN_ENV: &str = "TWINNING_PSQL_BIN";
const MATERIALIZATION_METHOD: &str = "psql_copy_stdout";
const NULL_SENTINEL: &str = "\\N";

#[derive(Debug, Clone, PartialEq)]
pub struct SourceCapture {
    pub relations: SnapshotRelations,
    pub report: SourceMaterializationReport,
}

pub fn capture_source_relations(
    catalog: &Catalog,
    source_url: &str,
) -> RefusalResult<SourceCapture> {
    let mut relations = BTreeMap::new();
    let mut table_rows = BTreeMap::new();

    for table in &catalog.tables {
        let rows = capture_table(source_url, table)?;
        table_rows.insert(table.name.clone(), rows.len() as u64);
        relations.insert(table.name.clone(), rows);
    }

    let row_count = table_rows.values().copied().sum();
    Ok(SourceCapture {
        relations,
        report: SourceMaterializationReport {
            source_identity: format!("sha256:{}", sha256_hex(source_url.as_bytes())),
            method: MATERIALIZATION_METHOD.to_owned(),
            table_count: catalog.tables.len(),
            row_count,
            tables: table_rows,
        },
    })
}

fn capture_table(source_url: &str, table: &TableCatalog) -> RefusalResult<Vec<SnapshotRow>> {
    let sql = copy_sql(table)?;
    let output = run_psql(
        source_url,
        &["--set=ON_ERROR_STOP=1", &format!("--command={sql}")],
    )?;
    let stdout = psql_success_stdout(output, &format!("capture table `{}`", table.name))?;
    let records = parse_csv_records(&stdout).map_err(|error| {
        Box::new(refusal::runtime_io(
            "source_materialization_parse_csv",
            format!("{}: {error}", table.name),
        ))
    })?;
    let (headers, data_rows) = records.split_first().ok_or_else(|| {
        Box::new(refusal::runtime_io(
            "source_materialization_parse_csv",
            format!("{}: missing CSV header", table.name),
        ))
    })?;
    let expected_headers = table
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    if headers != &expected_headers {
        return Err(Box::new(refusal::runtime_io(
            "source_materialization_shape",
            format!(
                "{}: source columns [{}] did not match catalog columns [{}]",
                table.name,
                headers.join(", "),
                expected_headers.join(", ")
            ),
        )));
    }

    data_rows
        .iter()
        .enumerate()
        .map(|(row_index, row)| {
            if row.len() != table.columns.len() {
                return Err(Box::new(refusal::runtime_io(
                    "source_materialization_shape",
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
                .map(|(column, raw)| {
                    let raw = if raw == NULL_SENTINEL {
                        None
                    } else {
                        Some(raw.as_str())
                    };
                    let value = kernel_value_from_source(raw, &column.normalized_type).map_err(
                        |error| {
                            Box::new(refusal::runtime_io(
                                "source_materialization_value",
                                format!("{}.{}: {error}", table.name, column.name),
                            ))
                        },
                    )?;
                    let value = serde_json::to_value(value)
                        .map_err(|error| Box::new(refusal::serialization(error.to_string())))?;
                    Ok((column.name.clone(), value))
                })
                .collect::<RefusalResult<SnapshotRow>>()
        })
        .collect()
}

fn copy_sql(table: &TableCatalog) -> RefusalResult<String> {
    let select_columns = table
        .columns
        .iter()
        .map(|column| quote_identifier(&column.name))
        .collect::<Vec<_>>()
        .join(", ");
    let order_column_names = table
        .primary_key
        .as_ref()
        .map(|primary_key| primary_key.columns.clone())
        .unwrap_or_else(|| {
            table
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect()
        });
    let order_columns = order_column_names
        .iter()
        .map(|column| quote_identifier(column))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!(
        "COPY (SELECT {select_columns} FROM {} ORDER BY {order_columns}) TO STDOUT WITH (FORMAT CSV, HEADER, NULL '{NULL_SENTINEL}')",
        quote_qualified_table_name(&table.name)?
    ))
}

fn quote_qualified_table_name(table_name: &str) -> RefusalResult<String> {
    let parts = table_name.split('.').collect::<Vec<_>>();
    if parts.len() != 2 || parts.iter().any(|part| part.is_empty()) {
        return Err(Box::new(refusal::runtime_io(
            "source_materialization_shape",
            format!("catalog table name `{table_name}` is not schema-qualified"),
        )));
    }
    Ok(format!(
        "{}.{}",
        quote_identifier(parts[0]),
        quote_identifier(parts[1])
    ))
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn run_psql(source_url: &str, args: &[&str]) -> RefusalResult<Output> {
    Command::new(psql_bin())
        .arg("-X")
        .arg("--dbname")
        .arg(source_url)
        .args(args)
        .output()
        .map_err(|error| {
            Box::new(refusal::runtime_io(
                "source_materialization_psql",
                error.to_string(),
            ))
        })
}

fn psql_bin() -> OsString {
    std::env::var_os(PSQL_BIN_ENV).unwrap_or_else(|| OsString::from("psql"))
}

fn psql_success_stdout(output: Output, context: &str) -> RefusalResult<String> {
    let stdout = String::from_utf8(output.stdout).map_err(|error| {
        Box::new(refusal::runtime_io(
            "source_materialization_psql",
            format!("{context}: stdout was not UTF-8: {error}"),
        ))
    })?;
    if output.status.success() {
        return Ok(stdout);
    }
    let stderr = String::from_utf8(output.stderr).map_err(|error| {
        Box::new(refusal::runtime_io(
            "source_materialization_psql",
            format!("{context}: stderr was not UTF-8: {error}"),
        ))
    })?;
    Err(Box::new(refusal::runtime_io(
        "source_materialization_psql",
        format!("{context}: stdout={stdout}; stderr={stderr}"),
    )))
}

fn kernel_value_from_source(
    raw: Option<&str>,
    normalized_type: &str,
) -> Result<KernelValue, String> {
    let Some(raw) = raw else {
        return Ok(KernelValue::Null);
    };
    let value_type = ValueType::from_normalized_catalog_type(normalized_type)
        .map_err(|error| error.to_string())?;
    match value_type {
        ValueType::Text => Ok(KernelValue::Text(raw.to_owned())),
        ValueType::Integer => raw
            .parse::<i32>()
            .map(KernelValue::Integer)
            .map_err(|error| error.to_string()),
        ValueType::Smallint => raw
            .parse::<i16>()
            .map(KernelValue::Smallint)
            .map_err(|error| error.to_string()),
        ValueType::Bigint => raw
            .parse::<i64>()
            .map(KernelValue::Bigint)
            .map_err(|error| error.to_string()),
        ValueType::Numeric => Ok(KernelValue::Numeric(raw.to_owned())),
        ValueType::Float => raw
            .parse::<f64>()
            .map(KernelValue::Float)
            .map_err(|error| error.to_string()),
        ValueType::Boolean => parse_postgres_bool(raw).map(KernelValue::Boolean),
        ValueType::Timestamp => Ok(KernelValue::Timestamp(raw.to_owned())),
        ValueType::Date => Ok(KernelValue::Date(raw.to_owned())),
        ValueType::Json => serde_json::from_str::<JsonValue>(raw)
            .map(KernelValue::Json)
            .map_err(|error| error.to_string()),
        ValueType::Bytes => parse_postgres_bytea(raw).map(KernelValue::Bytes),
        ValueType::Array => Err(String::from(
            "array source materialization is outside the declared v0 subset",
        )),
    }
}

fn parse_postgres_bool(raw: &str) -> Result<bool, String> {
    match raw {
        "t" | "true" => Ok(true),
        "f" | "false" => Ok(false),
        other => Err(format!("invalid Postgres boolean `{other}`")),
    }
}

fn parse_postgres_bytea(raw: &str) -> Result<Vec<u8>, String> {
    let hex = raw
        .strip_prefix("\\x")
        .ok_or_else(|| format!("unsupported bytea output `{raw}`"))?;
    if hex.len() % 2 != 0 {
        return Err(format!("bytea hex output has odd length: `{raw}`"));
    }
    (0..hex.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&hex[index..index + 2], 16).map_err(|error| error.to_string())
        })
        .collect()
}

fn parse_csv_records(input: &str) -> Result<Vec<Vec<String>>, String> {
    let mut records = Vec::new();
    let mut row = Vec::new();
    let mut field = String::new();
    let mut chars = input.chars().peekable();
    let mut in_quotes = false;
    let mut saw_anything = false;

    while let Some(ch) = chars.next() {
        saw_anything = true;
        match ch {
            '"' if in_quotes && chars.peek() == Some(&'"') => {
                field.push('"');
                chars.next();
            }
            '"' => in_quotes = !in_quotes,
            ',' if !in_quotes => row.push(std::mem::take(&mut field)),
            '\n' if !in_quotes => {
                row.push(std::mem::take(&mut field));
                records.push(std::mem::take(&mut row));
            }
            '\r' if !in_quotes => {
                if chars.peek() == Some(&'\n') {
                    chars.next();
                }
                row.push(std::mem::take(&mut field));
                records.push(std::mem::take(&mut row));
            }
            _ => field.push(ch),
        }
    }

    if in_quotes {
        return Err(String::from("unterminated quoted CSV field"));
    }
    if saw_anything && (!field.is_empty() || !row.is_empty()) {
        row.push(field);
        records.push(row);
    }
    Ok(records)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use crate::catalog::parse_postgres_schema;

    use super::{copy_sql, kernel_value_from_source, parse_csv_records};

    #[test]
    fn copy_sql_quotes_declared_table_and_columns() {
        let catalog = parse_postgres_schema(
            r#"
            CREATE TABLE public.deals (
                deal_id TEXT PRIMARY KEY,
                amount INTEGER
            );
            "#,
        )
        .expect("parse schema");
        let sql = copy_sql(&catalog.tables[0]).expect("copy sql");

        assert_eq!(
            sql,
            r#"COPY (SELECT "deal_id", "amount" FROM "public"."deals" ORDER BY "deal_id") TO STDOUT WITH (FORMAT CSV, HEADER, NULL '\N')"#
        );
    }

    #[test]
    fn csv_parser_handles_quoted_commas_and_crlf() {
        let records = parse_csv_records("id,name\r\n1,\"Alpha, Inc.\"\r\n").expect("parse csv");

        assert_eq!(
            records,
            vec![
                vec![String::from("id"), String::from("name")],
                vec![String::from("1"), String::from("Alpha, Inc.")],
            ]
        );
    }

    #[test]
    fn source_values_follow_declared_kernel_types() {
        assert_eq!(
            kernel_value_from_source(Some("42"), "integer").expect("integer"),
            crate::kernel::value::KernelValue::Integer(42)
        );
        assert_eq!(
            kernel_value_from_source(Some("t"), "boolean").expect("bool"),
            crate::kernel::value::KernelValue::Boolean(true)
        );
        assert_eq!(
            kernel_value_from_source(None, "text").expect("null"),
            crate::kernel::value::KernelValue::Null
        );
    }
}
