//! Snowflake DDL catalog normalization.

use std::{collections::BTreeMap, fs, path::Path};

use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};
use sha2::{Digest, Sha256};
use sqlparser::{
    ast::{
        CharacterLength, ColumnDef, ColumnOption, DataType, ExactNumberInfo, Expr, Ident, Insert,
        ObjectName, SetExpr, Statement, TimezoneInfo, UnaryOperator, Value,
    },
    dialect::SnowflakeDialect,
    parser::Parser,
};
use twinning_kernel::refusal::{self, RefusalResult};

pub const DEFAULT_DATABASE_NAME: &str = "TWINDB";
pub const DEFAULT_SCHEMA_NAME: &str = "PUBLIC";
pub const DEFAULT_WAREHOUSE_NAME: &str = "TWIN_WH";
pub const DEFAULT_ROLE_NAME: &str = "SYSADMIN";
pub const DEFAULT_USER_NAME: &str = "TWIN_USER";
pub const DEFAULT_VERSION: &str = "8.40.2";

const DEFAULT_TEXT_LENGTH: i64 = 16_777_216;
const DEFAULT_BINARY_LENGTH: i64 = 8_388_608;
const DEFAULT_NUMBER_PRECISION: i64 = 38;
const DEFAULT_NUMBER_SCALE: i64 = 0;
const DEFAULT_TEMPORAL_SCALE: i64 = 9;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnowflakeColumn {
    pub name: String,
    pub sf_type: SnowflakeType,
    pub nullable: bool,
    pub byte_length: Option<i64>,
    pub char_length: Option<i64>,
    pub precision: Option<i64>,
    pub scale: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnowflakeTable {
    pub name: String,
    pub columns: Vec<SnowflakeColumn>,
    pub rows: Vec<Vec<serde_json::Value>>,
}

impl SnowflakeTable {
    pub fn column(&self, name: &str) -> Option<&SnowflakeColumn> {
        let normalized = normalize_lookup_identifier(name);
        self.columns
            .iter()
            .find(|column| column.name == normalized || column.name == name)
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        let normalized = normalize_lookup_identifier(name);
        self.columns
            .iter()
            .position(|column| column.name == normalized || column.name == name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnowflakeCatalog {
    pub source: String,
    pub database_name: String,
    pub schema_name: String,
    pub tables: Vec<SnowflakeTable>,
    pub catalog_hash: String,
    pub source_materialization: Option<SnowflakeSourceMaterialization>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnowflakeSourceMaterialization {
    pub source_identity: String,
    pub method: String,
    pub table_count: usize,
    pub row_count: usize,
    pub tables: BTreeMap<String, usize>,
}

impl SnowflakeCatalog {
    pub fn from_schema_path(path: Option<&Path>) -> RefusalResult<Self> {
        let Some(path) = path else {
            return Ok(Self::empty());
        };

        let source =
            fs::read_to_string(path).map_err(|error| Box::new(refusal::io_read(path, &error)))?;
        Self::from_ddl(&source).map_err(|error| Box::new(refusal::schema_parse(path, error)))
    }

    pub fn from_ddl(source: &str) -> Result<Self, String> {
        let dialect = SnowflakeDialect {};
        let statements = Parser::parse_sql(&dialect, source).map_err(|error| error.to_string())?;
        let mut tables = Vec::new();

        for statement in &statements {
            if let Statement::CreateTable(create_table) = statement {
                let name = table_name(&create_table.name)?;
                let mut columns = Vec::new();
                for column in &create_table.columns {
                    columns.push(column_from_def(column)?);
                }
                tables.push(SnowflakeTable {
                    name,
                    columns,
                    rows: Vec::new(),
                });
            }
        }

        for statement in &statements {
            if let Statement::Insert(insert) = statement {
                append_insert_rows(&mut tables, insert)?;
            }
        }

        let catalog_hash = catalog_hash(&tables)?;
        Ok(Self {
            source: "schema_file".to_owned(),
            database_name: DEFAULT_DATABASE_NAME.to_owned(),
            schema_name: DEFAULT_SCHEMA_NAME.to_owned(),
            tables,
            catalog_hash,
            source_materialization: None,
        })
    }

    pub fn empty() -> Self {
        Self {
            source: "empty".to_owned(),
            database_name: DEFAULT_DATABASE_NAME.to_owned(),
            schema_name: DEFAULT_SCHEMA_NAME.to_owned(),
            tables: Vec::new(),
            catalog_hash: catalog_hash(&[]).expect("empty Snowflake catalog hash renders"),
            source_materialization: None,
        }
    }

    pub fn table(&self, name: &str) -> Option<&SnowflakeTable> {
        let normalized = normalize_lookup_identifier(name);
        self.tables
            .iter()
            .find(|table| table.name == normalized || table.name == name)
    }

    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    pub fn row_count(&self) -> usize {
        self.tables.iter().map(|table| table.rows.len()).sum()
    }

    pub fn replace_rows(
        &mut self,
        rows_by_table: BTreeMap<String, Vec<Vec<serde_json::Value>>>,
        materialization: SnowflakeSourceMaterialization,
    ) -> Result<(), String> {
        for (table_name, rows) in rows_by_table {
            let table = self
                .tables
                .iter_mut()
                .find(|table| table.name == table_name)
                .ok_or_else(|| {
                    format!("materialized source returned unknown table `{table_name}`")
                })?;
            table.rows = rows;
        }
        self.source_materialization = Some(materialization);
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnowflakeType {
    Fixed { precision: i64, scale: i64 },
    Real,
    Text { char_length: i64, byte_length: i64 },
    Boolean,
    Date,
    Time { scale: i64 },
    TimestampNtz { scale: i64 },
    TimestampLtz { scale: i64 },
    TimestampTz { scale: i64 },
    Variant,
    Object,
    Array,
    Binary { byte_length: i64 },
    Geography,
    Geometry,
}

impl SnowflakeType {
    pub fn from_data_type(data_type: &DataType) -> Result<Self, String> {
        match data_type {
            DataType::Numeric(info) | DataType::Decimal(info) | DataType::Dec(info) => {
                let (precision, scale) = exact_number_info(info)?;
                Ok(Self::Fixed { precision, scale })
            }
            DataType::Int(_)
            | DataType::Integer(_)
            | DataType::BigInt(_)
            | DataType::SmallInt(_)
            | DataType::TinyInt(_)
            | DataType::Int2(_)
            | DataType::Int4(_)
            | DataType::Int8(_)
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64 => Ok(integer_alias_type()),
            DataType::Float(_)
            | DataType::Float4
            | DataType::Float8
            | DataType::Float32
            | DataType::Float64
            | DataType::Real
            | DataType::Double
            | DataType::DoublePrecision => Ok(Self::Real),
            DataType::Character(length)
            | DataType::Char(length)
            | DataType::CharacterVarying(length)
            | DataType::CharVarying(length)
            | DataType::Varchar(length)
            | DataType::Nvarchar(length) => text_type(character_length(*length)?),
            DataType::String(length) => {
                text_type(optional_u64_length(*length, DEFAULT_TEXT_LENGTH)?)
            }
            DataType::Text => text_type(DEFAULT_TEXT_LENGTH),
            DataType::Binary(length) | DataType::Varbinary(length) => Ok(Self::Binary {
                byte_length: optional_u64_length(*length, DEFAULT_BINARY_LENGTH)?,
            }),
            DataType::Bool | DataType::Boolean => Ok(Self::Boolean),
            DataType::Date => Ok(Self::Date),
            DataType::Time(scale, _) => Ok(Self::Time {
                scale: optional_u64_length(*scale, DEFAULT_TEMPORAL_SCALE)?,
            }),
            DataType::Datetime(scale) => Ok(Self::TimestampNtz {
                scale: optional_u64_length(*scale, DEFAULT_TEMPORAL_SCALE)?,
            }),
            DataType::Timestamp(scale, timezone) => match timezone {
                TimezoneInfo::WithTimeZone | TimezoneInfo::Tz => Ok(Self::TimestampTz {
                    scale: optional_u64_length(*scale, DEFAULT_TEMPORAL_SCALE)?,
                }),
                TimezoneInfo::None | TimezoneInfo::WithoutTimeZone => Ok(Self::TimestampNtz {
                    scale: optional_u64_length(*scale, DEFAULT_TEMPORAL_SCALE)?,
                }),
            },
            DataType::Array(_) => Ok(Self::Array),
            DataType::Custom(name, modifiers) => {
                Self::from_type_name(&object_name(name), modifiers)
            }
            _ => Self::from_type_name(&data_type.to_string(), &[]),
        }
    }

    pub fn type_string(&self) -> &'static str {
        match self {
            Self::Fixed { .. } => "fixed",
            Self::Real => "real",
            Self::Text { .. } => "text",
            Self::Boolean => "boolean",
            Self::Date => "date",
            Self::Time { .. } => "time",
            Self::TimestampNtz { .. } => "timestamp_ntz",
            Self::TimestampLtz { .. } => "timestamp_ltz",
            Self::TimestampTz { .. } => "timestamp_tz",
            Self::Variant => "variant",
            Self::Object => "object",
            Self::Array => "array",
            Self::Binary { .. } => "binary",
            Self::Geography => "geography",
            Self::Geometry => "geometry",
        }
    }

    pub fn byte_length(&self) -> Option<i64> {
        match self {
            Self::Text { byte_length, .. } | Self::Binary { byte_length } => Some(*byte_length),
            _ => None,
        }
    }

    pub fn char_length(&self) -> Option<i64> {
        match self {
            Self::Text { char_length, .. } => Some(*char_length),
            Self::Binary { byte_length } => Some(*byte_length),
            _ => None,
        }
    }

    pub fn precision(&self) -> Option<i64> {
        match self {
            Self::Fixed { precision, .. } => Some(*precision),
            Self::Time { .. }
            | Self::TimestampNtz { .. }
            | Self::TimestampLtz { .. }
            | Self::TimestampTz { .. } => Some(0),
            _ => None,
        }
    }

    pub fn scale(&self) -> Option<i64> {
        match self {
            Self::Fixed { scale, .. }
            | Self::Time { scale }
            | Self::TimestampNtz { scale }
            | Self::TimestampLtz { scale }
            | Self::TimestampTz { scale } => Some(*scale),
            _ => None,
        }
    }

    pub fn ddl_type(&self) -> String {
        match self {
            Self::Fixed { precision, scale } => format!("NUMBER({precision},{scale})"),
            Self::Real => "FLOAT".to_owned(),
            Self::Text { char_length, .. } => format!("VARCHAR({char_length})"),
            Self::Boolean => "BOOLEAN".to_owned(),
            Self::Date => "DATE".to_owned(),
            Self::Time { scale } => format!("TIME({scale})"),
            Self::TimestampNtz { scale } => format!("TIMESTAMP_NTZ({scale})"),
            Self::TimestampLtz { scale } => format!("TIMESTAMP_LTZ({scale})"),
            Self::TimestampTz { scale } => format!("TIMESTAMP_TZ({scale})"),
            Self::Variant => "VARIANT".to_owned(),
            Self::Object => "OBJECT".to_owned(),
            Self::Array => "ARRAY".to_owned(),
            Self::Binary { byte_length } => format!("BINARY({byte_length})"),
            Self::Geography => "GEOGRAPHY".to_owned(),
            Self::Geometry => "GEOMETRY".to_owned(),
        }
    }

    pub fn show_data_type_json(&self, nullable: bool) -> String {
        match self {
            Self::Fixed { precision, scale } => {
                format!(
                    r#"{{"type":"FIXED","precision":{precision},"scale":{scale},"nullable":{nullable}}}"#
                )
            }
            Self::Real => format!(r#"{{"type":"REAL","nullable":{nullable}}}"#),
            Self::Text {
                char_length,
                byte_length,
            } => {
                format!(
                    r#"{{"type":"TEXT","length":{char_length},"byteLength":{byte_length},"nullable":{nullable},"fixed":false}}"#
                )
            }
            Self::Boolean => format!(r#"{{"type":"BOOLEAN","nullable":{nullable}}}"#),
            Self::Date => format!(r#"{{"type":"DATE","nullable":{nullable}}}"#),
            Self::Time { scale } => {
                format!(r#"{{"type":"TIME","precision":0,"scale":{scale},"nullable":{nullable}}}"#)
            }
            Self::TimestampNtz { scale } => {
                format!(
                    r#"{{"type":"TIMESTAMP_NTZ","precision":0,"scale":{scale},"nullable":{nullable}}}"#
                )
            }
            Self::TimestampLtz { scale } => {
                format!(
                    r#"{{"type":"TIMESTAMP_LTZ","precision":0,"scale":{scale},"nullable":{nullable}}}"#
                )
            }
            Self::TimestampTz { scale } => {
                format!(
                    r#"{{"type":"TIMESTAMP_TZ","precision":0,"scale":{scale},"nullable":{nullable}}}"#
                )
            }
            Self::Variant => format!(r#"{{"type":"VARIANT","nullable":{nullable}}}"#),
            Self::Object => format!(r#"{{"type":"OBJECT","nullable":{nullable}}}"#),
            Self::Array => format!(r#"{{"type":"ARRAY","nullable":{nullable}}}"#),
            Self::Binary { byte_length } => {
                format!(r#"{{"type":"BINARY","byteLength":{byte_length},"nullable":{nullable}}}"#)
            }
            Self::Geography => format!(r#"{{"type":"GEOGRAPHY","nullable":{nullable}}}"#),
            Self::Geometry => format!(r#"{{"type":"GEOMETRY","nullable":{nullable}}}"#),
        }
    }

    fn from_type_name(raw_name: &str, modifiers: &[String]) -> Result<Self, String> {
        let (name, inline_modifiers) = split_type_name(raw_name);
        let parsed_modifiers = if modifiers.is_empty() {
            inline_modifiers
        } else {
            parse_modifiers(modifiers)
        };

        match name.as_str() {
            "NUMBER" | "DECIMAL" | "NUMERIC" => {
                let precision = parsed_modifiers
                    .first()
                    .copied()
                    .unwrap_or(DEFAULT_NUMBER_PRECISION);
                let scale = parsed_modifiers
                    .get(1)
                    .copied()
                    .unwrap_or(DEFAULT_NUMBER_SCALE);
                Ok(Self::Fixed { precision, scale })
            }
            "INT" | "INTEGER" | "BIGINT" | "SMALLINT" | "TINYINT" | "BYTEINT" => {
                Ok(integer_alias_type())
            }
            "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" | "DOUBLE PRECISION" | "REAL" => {
                Ok(Self::Real)
            }
            "VARCHAR" | "CHAR" | "CHARACTER" | "STRING" | "TEXT" | "NCHAR" | "NVARCHAR" => {
                text_type(
                    parsed_modifiers
                        .first()
                        .copied()
                        .unwrap_or(DEFAULT_TEXT_LENGTH),
                )
            }
            "BINARY" | "VARBINARY" => Ok(Self::Binary {
                byte_length: parsed_modifiers
                    .first()
                    .copied()
                    .unwrap_or(DEFAULT_BINARY_LENGTH),
            }),
            "BOOLEAN" | "BOOL" => Ok(Self::Boolean),
            "DATE" => Ok(Self::Date),
            "TIME" => Ok(Self::Time {
                scale: parsed_modifiers
                    .first()
                    .copied()
                    .unwrap_or(DEFAULT_TEMPORAL_SCALE),
            }),
            "TIMESTAMP" | "DATETIME" | "TIMESTAMP_NTZ" => Ok(Self::TimestampNtz {
                scale: parsed_modifiers
                    .first()
                    .copied()
                    .unwrap_or(DEFAULT_TEMPORAL_SCALE),
            }),
            "TIMESTAMP_LTZ" | "TIMESTAMPLTZ" => Ok(Self::TimestampLtz {
                scale: parsed_modifiers
                    .first()
                    .copied()
                    .unwrap_or(DEFAULT_TEMPORAL_SCALE),
            }),
            "TIMESTAMP_TZ" | "TIMESTAMPTZ" => Ok(Self::TimestampTz {
                scale: parsed_modifiers
                    .first()
                    .copied()
                    .unwrap_or(DEFAULT_TEMPORAL_SCALE),
            }),
            "VARIANT" => Ok(Self::Variant),
            "OBJECT" => Ok(Self::Object),
            "ARRAY" => Ok(Self::Array),
            "GEOGRAPHY" => Ok(Self::Geography),
            "GEOMETRY" => Ok(Self::Geometry),
            _ => Err(format!("unsupported Snowflake DDL type `{raw_name}`")),
        }
    }
}

pub fn normalize_identifier(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_ascii_uppercase()
    }
}

pub fn normalize_lookup_identifier(raw: &str) -> String {
    let trimmed = raw.trim();
    let trimmed = trimmed.strip_suffix(';').unwrap_or(trimmed);
    if let Some(unquoted) = trimmed
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
    {
        unquoted.replace("\"\"", "\"")
    } else {
        trimmed.to_ascii_uppercase()
    }
}

fn column_from_def(column: &ColumnDef) -> Result<SnowflakeColumn, String> {
    let sf_type = SnowflakeType::from_data_type(&column.data_type)?;
    let nullable = !column
        .options
        .iter()
        .any(|option| matches!(option.option, ColumnOption::NotNull));

    Ok(SnowflakeColumn {
        name: normalize_identifier(&column.name),
        byte_length: sf_type.byte_length(),
        char_length: sf_type.char_length(),
        precision: sf_type.precision(),
        scale: sf_type.scale(),
        sf_type,
        nullable,
    })
}

fn table_name(name: &ObjectName) -> Result<String, String> {
    name.0
        .last()
        .map(normalize_identifier)
        .ok_or_else(|| "CREATE TABLE statement had no table name".to_owned())
}

fn append_insert_rows(tables: &mut [SnowflakeTable], insert: &Insert) -> Result<(), String> {
    let name = table_name(&insert.table_name)?;
    let table = tables
        .iter_mut()
        .find(|table| table.name == name)
        .ok_or_else(|| format!("INSERT referenced unknown table `{name}`"))?;
    let Some(source) = &insert.source else {
        return Err(format!("INSERT into `{name}` had no source rows"));
    };
    let SetExpr::Values(values) = source.body.as_ref() else {
        return Err(format!(
            "INSERT into `{name}` used a non-VALUES source, which Snowflake twin fixtures do not materialize"
        ));
    };

    let insert_columns = if insert.columns.is_empty() {
        table
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>()
    } else {
        insert.columns.iter().map(normalize_identifier).collect()
    };
    let target_indexes = insert_columns
        .iter()
        .map(|column| {
            table
                .column_index(column)
                .ok_or_else(|| format!("INSERT into `{name}` referenced unknown column `{column}`"))
        })
        .collect::<Result<Vec<_>, _>>()?;

    for row in &values.rows {
        if row.len() != target_indexes.len() {
            return Err(format!(
                "INSERT into `{name}` had {} values for {} target columns",
                row.len(),
                target_indexes.len()
            ));
        }
        let mut materialized = vec![JsonValue::Null; table.columns.len()];
        for (expr, column_index) in row.iter().zip(target_indexes.iter().copied()) {
            materialized[column_index] =
                expr_to_json_value(expr, &table.columns[column_index].sf_type)?;
        }
        table.rows.push(materialized);
    }

    Ok(())
}

fn expr_to_json_value(expr: &Expr, sf_type: &SnowflakeType) -> Result<JsonValue, String> {
    match expr {
        Expr::Value(value) => sql_value_to_json(value, sf_type),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match expr.as_ref() {
            Expr::Value(Value::Number(raw, long)) => {
                sql_value_to_json(&Value::Number(format!("-{raw}"), *long), sf_type)
            }
            _ => Err(format!("unsupported negative fixture value `{expr}`")),
        },
        _ => Err(format!("unsupported fixture value expression `{expr}`")),
    }
}

fn sql_value_to_json(value: &Value, sf_type: &SnowflakeType) -> Result<JsonValue, String> {
    match value {
        Value::Number(raw, _) => numeric_json_value(raw, sf_type),
        Value::SingleQuotedString(value)
        | Value::DoubleQuotedString(value)
        | Value::NationalStringLiteral(value) => Ok(json!(value)),
        Value::Boolean(value) => Ok(json!(value)),
        Value::Null => Ok(JsonValue::Null),
        _ => Err(format!("unsupported fixture value `{value}`")),
    }
}

fn numeric_json_value(raw: &str, sf_type: &SnowflakeType) -> Result<JsonValue, String> {
    match sf_type {
        SnowflakeType::Fixed { scale, .. } if *scale == 0 => raw
            .parse::<i64>()
            .map(JsonValue::from)
            .map_err(|error| format!("integer fixture value `{raw}` did not parse: {error}")),
        SnowflakeType::Date
        | SnowflakeType::Time { .. }
        | SnowflakeType::TimestampNtz { .. }
        | SnowflakeType::TimestampLtz { .. }
        | SnowflakeType::TimestampTz { .. } => raw
            .parse::<i64>()
            .map(JsonValue::from)
            .map_err(|error| format!("integer fixture value `{raw}` did not parse: {error}")),
        SnowflakeType::Real => raw
            .parse::<f64>()
            .map(JsonValue::from)
            .map_err(|error| format!("float fixture value `{raw}` did not parse: {error}")),
        SnowflakeType::Fixed { .. } => Ok(json!(raw)),
        _ => Ok(json!(raw)),
    }
}

fn object_name(name: &ObjectName) -> String {
    name.0
        .iter()
        .map(normalize_identifier)
        .collect::<Vec<_>>()
        .join(".")
}

fn catalog_hash(tables: &[SnowflakeTable]) -> Result<String, String> {
    #[derive(Serialize)]
    struct HashTable<'a> {
        name: &'a str,
        columns: &'a [SnowflakeColumn],
    }

    let hash_tables = tables
        .iter()
        .map(|table| HashTable {
            name: &table.name,
            columns: &table.columns,
        })
        .collect::<Vec<_>>();
    let rendered = serde_json::to_vec(&hash_tables).map_err(|error| error.to_string())?;
    let mut hasher = Sha256::new();
    hasher.update(rendered);
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn exact_number_info(info: &ExactNumberInfo) -> Result<(i64, i64), String> {
    match info {
        ExactNumberInfo::None => Ok((DEFAULT_NUMBER_PRECISION, DEFAULT_NUMBER_SCALE)),
        ExactNumberInfo::Precision(precision) => {
            Ok((u64_to_i64(*precision, "precision")?, DEFAULT_NUMBER_SCALE))
        }
        ExactNumberInfo::PrecisionAndScale(precision, scale) => Ok((
            u64_to_i64(*precision, "precision")?,
            u64_to_i64(*scale, "scale")?,
        )),
    }
}

fn character_length(length: Option<CharacterLength>) -> Result<i64, String> {
    match length {
        Some(CharacterLength::IntegerLength { length, .. }) => u64_to_i64(length, "length"),
        Some(CharacterLength::Max) | None => Ok(DEFAULT_TEXT_LENGTH),
    }
}

fn optional_u64_length(value: Option<u64>, default: i64) -> Result<i64, String> {
    value.map_or(Ok(default), |value| u64_to_i64(value, "length"))
}

fn u64_to_i64(value: u64, name: &str) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| format!("Snowflake {name} `{value}` exceeds i64"))
}

fn text_type(length: i64) -> Result<SnowflakeType, String> {
    Ok(SnowflakeType::Text {
        char_length: length,
        byte_length: length,
    })
}

fn integer_alias_type() -> SnowflakeType {
    SnowflakeType::Fixed {
        precision: DEFAULT_NUMBER_PRECISION,
        scale: DEFAULT_NUMBER_SCALE,
    }
}

fn split_type_name(raw: &str) -> (String, Vec<i64>) {
    let trimmed = raw.trim();
    let (name, modifiers) = if let Some((name, rest)) = trimmed.split_once('(') {
        (
            name,
            rest.rsplit_once(')').map(|(body, _)| body).unwrap_or(rest),
        )
    } else {
        (trimmed, "")
    };

    let normalized_name = name
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_uppercase();
    let parsed_modifiers = modifiers
        .split(',')
        .filter_map(|part| part.trim().parse::<i64>().ok())
        .collect();
    (normalized_name, parsed_modifiers)
}

fn parse_modifiers(modifiers: &[String]) -> Vec<i64> {
    modifiers
        .iter()
        .flat_map(|modifier| modifier.split(','))
        .filter_map(|part| {
            part.trim()
                .trim_start_matches('(')
                .trim_end_matches(')')
                .parse::<i64>()
                .ok()
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_snowflake_ddl_type_variants() {
        let ddl = r#"
            CREATE TABLE facts (
                amount NUMBER(10,2),
                default_amount NUMBER,
                id INT NOT NULL,
                ratio FLOAT,
                ratio4 FLOAT4,
                ratio8 FLOAT8,
                precise DOUBLE PRECISION,
                name VARCHAR(20),
                code CHAR(3),
                description STRING,
                note TEXT,
                national NCHAR(4),
                payload VARIANT,
                obj OBJECT,
                tags ARRAY,
                data BINARY(16),
                blob VARBINARY,
                active BOOLEAN,
                event_date DATE,
                event_time TIME(3),
                created_at TIMESTAMP_NTZ(6),
                local_at TIMESTAMP_LTZ,
                remote_at TIMESTAMP_TZ,
                geo GEOGRAPHY,
                geom GEOMETRY
            );
        "#;

        let catalog = SnowflakeCatalog::from_ddl(ddl).expect("DDL parses");
        let table = catalog.table("facts").expect("facts table");

        assert_eq!(
            table.column("amount").unwrap().sf_type,
            SnowflakeType::Fixed {
                precision: 10,
                scale: 2
            }
        );
        assert_eq!(
            table.column("default_amount").unwrap().sf_type,
            SnowflakeType::Fixed {
                precision: 38,
                scale: 0
            }
        );
        assert_eq!(
            table.column("id").unwrap().sf_type,
            SnowflakeType::Fixed {
                precision: 38,
                scale: 0
            }
        );
        assert_eq!(table.column("ratio").unwrap().sf_type, SnowflakeType::Real);
        assert_eq!(table.column("ratio4").unwrap().sf_type, SnowflakeType::Real);
        assert_eq!(table.column("ratio8").unwrap().sf_type, SnowflakeType::Real);
        assert_eq!(
            table.column("precise").unwrap().sf_type,
            SnowflakeType::Real
        );
        assert_eq!(
            table.column("name").unwrap().sf_type,
            SnowflakeType::Text {
                char_length: 20,
                byte_length: 20
            }
        );
        assert_eq!(
            table.column("description").unwrap().sf_type,
            SnowflakeType::Text {
                char_length: DEFAULT_TEXT_LENGTH,
                byte_length: DEFAULT_TEXT_LENGTH
            }
        );
        assert_eq!(
            table.column("data").unwrap().sf_type,
            SnowflakeType::Binary { byte_length: 16 }
        );
        assert_eq!(
            table.column("blob").unwrap().sf_type,
            SnowflakeType::Binary {
                byte_length: DEFAULT_BINARY_LENGTH
            }
        );
        assert_eq!(
            table.column("created_at").unwrap().sf_type,
            SnowflakeType::TimestampNtz { scale: 6 }
        );
        assert_eq!(
            table.column("local_at").unwrap().sf_type,
            SnowflakeType::TimestampLtz { scale: 9 }
        );
        assert_eq!(
            table.column("remote_at").unwrap().sf_type,
            SnowflakeType::TimestampTz { scale: 9 }
        );
        assert_eq!(
            table.column("geo").unwrap().sf_type,
            SnowflakeType::Geography
        );
        assert_eq!(
            table.column("geom").unwrap().sf_type,
            SnowflakeType::Geometry
        );
    }

    #[test]
    fn materializes_insert_values_from_schema_fixture() {
        let catalog = SnowflakeCatalog::from_ddl(
            r#"
            CREATE TABLE observations (
                id NUMBER(38,0),
                label VARCHAR(32),
                observed_on DATE,
                captured_at TIMESTAMP_NTZ(6)
            );
            INSERT INTO observations (id, label, observed_on, captured_at) VALUES
                (1, 'alpha', 19723, 1700000000000000),
                (2, 'beta', 19724, 1700000001000000);
            "#,
        )
        .expect("DDL and inserts parse");

        let table = catalog.table("observations").expect("table");
        assert_eq!(2, table.rows.len());
        assert_eq!(json!(1), table.rows[0][0]);
        assert_eq!(json!("alpha"), table.rows[0][1]);
        assert_eq!(json!(19723), table.rows[0][2]);
        assert_eq!(json!(1_700_000_000_000_000_i64), table.rows[0][3]);
        assert_eq!(2, catalog.row_count());
    }

    #[test]
    fn normalizes_unquoted_identifiers_and_preserves_quoted_identifiers() {
        let ddl = r#"CREATE TABLE "CamelTable" ("CamelColumn" VARCHAR, plain NUMBER);"#;
        let catalog = SnowflakeCatalog::from_ddl(ddl).expect("DDL parses");
        let table = catalog.table("\"CamelTable\"").expect("quoted table");

        assert_eq!(table.name, "CamelTable");
        assert!(table.column("\"CamelColumn\"").is_some());
        assert!(table.column("PLAIN").is_some());
    }

    #[test]
    fn catalog_hash_uses_canonical_table_json_not_raw_ddl() {
        let first = SnowflakeCatalog::from_ddl("CREATE TABLE t (id int);").unwrap();
        let second = SnowflakeCatalog::from_ddl("create table t (\n  id integer\n);").unwrap();

        assert_eq!(first.catalog_hash, second.catalog_hash);
        assert!(first.catalog_hash.starts_with("sha256:"));
    }
}
