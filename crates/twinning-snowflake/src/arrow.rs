//! Snowflake Arrow IPC rowset encoding.

use std::{collections::HashMap, sync::Arc};

use arrow_array::{
    ArrayRef, RecordBatch, StructArray,
    builder::{
        BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float64Builder,
        Int32Builder, Int64Builder, StringBuilder,
    },
};
use arrow_buffer::NullBufferBuilder;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, FieldRef, Fields, Schema};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use serde_json::Value as JsonValue;

use crate::{
    catalog::{SnowflakeColumn, SnowflakeType},
    rowtype::column_to_rowtype,
    show::SnowflakeResultColumn,
};

#[derive(Debug, Clone, PartialEq)]
pub struct EncodedQueryResult {
    pub rowset_base64: String,
    pub rowtype: Vec<JsonValue>,
}

pub fn encode_query_result(
    columns: &[SnowflakeResultColumn],
    rows: &[Vec<JsonValue>],
) -> Result<EncodedQueryResult, String> {
    let rowtype = columns.iter().map(result_column_to_rowtype).collect();
    if columns.is_empty() {
        return Ok(EncodedQueryResult {
            rowset_base64: String::new(),
            rowtype,
        });
    }

    let batch = build_record_batch(columns, rows)?;
    Ok(EncodedQueryResult {
        rowset_base64: to_ipc_base64(&batch)?,
        rowtype,
    })
}

pub fn build_record_batch(
    columns: &[SnowflakeResultColumn],
    rows: &[Vec<JsonValue>],
) -> Result<RecordBatch, String> {
    let schema = Arc::new(build_schema(columns));
    let arrays = columns
        .iter()
        .enumerate()
        .map(|(index, column)| build_column_array(column, rows, index))
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(schema, arrays)
        .map_err(|error| format!("failed to build Arrow record batch: {error}"))
}

pub fn build_schema(columns: &[SnowflakeResultColumn]) -> Schema {
    Schema::new(
        columns
            .iter()
            .map(|column| {
                Field::new(
                    column.name.clone(),
                    arrow_data_type(&column.sf_type),
                    column.nullable,
                )
                .with_metadata(field_metadata(column))
            })
            .collect::<Vec<_>>(),
    )
}

pub fn to_ipc_base64(batch: &RecordBatch) -> Result<String, String> {
    let mut stream = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut stream, batch.schema_ref())
            .map_err(|error| format!("failed to start Arrow IPC stream: {error}"))?;
        writer
            .write(batch)
            .map_err(|error| format!("failed to write Arrow IPC batch: {error}"))?;
        writer
            .finish()
            .map_err(|error| format!("failed to finish Arrow IPC stream: {error}"))?;
    }

    Ok(BASE64.encode(stream))
}

pub fn arrow_data_type(sf_type: &SnowflakeType) -> DataType {
    match sf_type {
        SnowflakeType::Fixed { precision, scale } if *scale == 0 && *precision <= 18 => {
            DataType::Int64
        }
        SnowflakeType::Fixed { precision, scale } => DataType::Decimal128(
            (*precision).try_into().unwrap_or(u8::MAX),
            (*scale).try_into().unwrap_or(i8::MAX),
        ),
        SnowflakeType::Real => DataType::Float64,
        SnowflakeType::Text { .. } => DataType::Utf8,
        SnowflakeType::Boolean => DataType::Boolean,
        SnowflakeType::Date => DataType::Date32,
        SnowflakeType::Time { .. } => DataType::Int64,
        SnowflakeType::TimestampNtz { .. } | SnowflakeType::TimestampLtz { .. } => {
            DataType::Struct(timestamp_fields(false))
        }
        SnowflakeType::TimestampTz { .. } => DataType::Struct(timestamp_fields(true)),
        SnowflakeType::Variant | SnowflakeType::Object | SnowflakeType::Array => DataType::Utf8,
        SnowflakeType::Binary { .. } => DataType::Binary,
        SnowflakeType::Geography | SnowflakeType::Geometry => DataType::Utf8,
    }
}

pub fn encode_timestamp_ntz(ts_micros: i64) -> (i64, i32) {
    let epoch = ts_micros.div_euclid(1_000_000);
    let fraction = (ts_micros.rem_euclid(1_000_000) * 1_000) as i32;
    (epoch, fraction)
}

pub fn encode_timestamp_tz(ts_micros: i64) -> (i64, i32, i32) {
    let (epoch, fraction) = encode_timestamp_ntz(ts_micros);
    (epoch, fraction, 1440)
}

fn result_column_to_rowtype(column: &SnowflakeResultColumn) -> JsonValue {
    let catalog_column = SnowflakeColumn {
        name: column.name.clone(),
        sf_type: column.sf_type.clone(),
        nullable: column.nullable,
        byte_length: column.sf_type.byte_length(),
        char_length: column.sf_type.char_length(),
        precision: column.sf_type.precision(),
        scale: column.sf_type.scale(),
    };
    column_to_rowtype(&catalog_column)
}

fn field_metadata(column: &SnowflakeResultColumn) -> HashMap<String, String> {
    HashMap::from([
        (
            "logicalType".to_owned(),
            column.sf_type.type_string().to_ascii_uppercase(),
        ),
        (
            "precision".to_owned(),
            column.sf_type.precision().unwrap_or(38).to_string(),
        ),
        (
            "scale".to_owned(),
            column.sf_type.scale().unwrap_or(0).to_string(),
        ),
        (
            "charLength".to_owned(),
            column.sf_type.char_length().unwrap_or(0).to_string(),
        ),
    ])
}

fn timestamp_fields(with_timezone: bool) -> Fields {
    let mut fields = vec![
        Arc::new(Field::new("epoch", DataType::Int64, false)),
        Arc::new(Field::new("fraction", DataType::Int32, false)),
    ];
    if with_timezone {
        fields.push(Arc::new(Field::new("timezone", DataType::Int32, false)));
    }
    Fields::from(fields)
}

fn build_column_array(
    column: &SnowflakeResultColumn,
    rows: &[Vec<JsonValue>],
    index: usize,
) -> Result<ArrayRef, String> {
    match column.sf_type {
        SnowflakeType::Fixed { precision, scale } if scale == 0 && precision <= 18 => {
            build_int64_array(index, rows)
        }
        SnowflakeType::Fixed { precision, scale } => {
            build_decimal128_array(index, rows, precision, scale)
        }
        SnowflakeType::Real => build_float64_array(index, rows),
        SnowflakeType::Text { .. }
        | SnowflakeType::Variant
        | SnowflakeType::Object
        | SnowflakeType::Array
        | SnowflakeType::Geography
        | SnowflakeType::Geometry => build_string_array(index, rows),
        SnowflakeType::Boolean => build_boolean_array(index, rows),
        SnowflakeType::Date => build_date32_array(index, rows),
        SnowflakeType::Time { .. } => build_int64_array(index, rows),
        SnowflakeType::TimestampNtz { .. } | SnowflakeType::TimestampLtz { .. } => {
            build_timestamp_struct_array(index, rows, false)
        }
        SnowflakeType::TimestampTz { .. } => build_timestamp_struct_array(index, rows, true),
        SnowflakeType::Binary { .. } => build_binary_array(index, rows),
    }
}

fn build_string_array(index: usize, rows: &[Vec<JsonValue>]) -> Result<ArrayRef, String> {
    let mut builder = StringBuilder::new();
    for row in rows {
        let value = row.get(index).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            builder.append_null();
        } else {
            builder.append_value(json_value_to_string(value)?);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_int64_array(index: usize, rows: &[Vec<JsonValue>]) -> Result<ArrayRef, String> {
    let mut builder = Int64Builder::new();
    for row in rows {
        let value = row.get(index).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            builder.append_null();
        } else {
            builder.append_value(json_value_to_i64(value)?);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_decimal128_array(
    index: usize,
    rows: &[Vec<JsonValue>],
    precision: i64,
    scale: i64,
) -> Result<ArrayRef, String> {
    let data_type = DataType::Decimal128(
        precision.try_into().unwrap_or(u8::MAX),
        scale.try_into().unwrap_or(i8::MAX),
    );
    let mut builder = Decimal128Builder::new().with_data_type(data_type);
    for row in rows {
        let value = row.get(index).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            builder.append_null();
        } else {
            builder.append_value(json_value_to_decimal(value, scale)?);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_float64_array(index: usize, rows: &[Vec<JsonValue>]) -> Result<ArrayRef, String> {
    let mut builder = Float64Builder::new();
    for row in rows {
        let value = row.get(index).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            builder.append_null();
        } else {
            builder.append_value(json_value_to_f64(value)?);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_boolean_array(index: usize, rows: &[Vec<JsonValue>]) -> Result<ArrayRef, String> {
    let mut builder = BooleanBuilder::new();
    for row in rows {
        let value = row.get(index).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            builder.append_null();
        } else {
            builder.append_value(json_value_to_bool(value)?);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_date32_array(index: usize, rows: &[Vec<JsonValue>]) -> Result<ArrayRef, String> {
    let mut builder = Date32Builder::new();
    for row in rows {
        let value = row.get(index).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            builder.append_null();
        } else {
            builder.append_value(json_value_to_i32(value)?);
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn build_timestamp_struct_array(
    index: usize,
    rows: &[Vec<JsonValue>],
    with_timezone: bool,
) -> Result<ArrayRef, String> {
    let mut epoch = Int64Builder::new();
    let mut fraction = Int32Builder::new();
    let mut timezone = Int32Builder::new();
    let mut nulls = NullBufferBuilder::new(rows.len());

    for row in rows {
        let value = row.get(index).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            epoch.append_value(0);
            fraction.append_value(0);
            timezone.append_value(0);
            nulls.append_null();
            continue;
        }

        let ts_micros = json_value_to_i64(value)?;
        let (encoded_epoch, encoded_fraction, encoded_timezone) = if with_timezone {
            encode_timestamp_tz(ts_micros)
        } else {
            let (encoded_epoch, encoded_fraction) = encode_timestamp_ntz(ts_micros);
            (encoded_epoch, encoded_fraction, 0)
        };
        epoch.append_value(encoded_epoch);
        fraction.append_value(encoded_fraction);
        timezone.append_value(encoded_timezone);
        nulls.append_non_null();
    }

    let mut fields_and_arrays = vec![
        (
            Arc::new(Field::new("epoch", DataType::Int64, false)) as FieldRef,
            Arc::new(epoch.finish()) as ArrayRef,
        ),
        (
            Arc::new(Field::new("fraction", DataType::Int32, false)) as FieldRef,
            Arc::new(fraction.finish()) as ArrayRef,
        ),
    ];
    if with_timezone {
        fields_and_arrays.push((
            Arc::new(Field::new("timezone", DataType::Int32, false)) as FieldRef,
            Arc::new(timezone.finish()) as ArrayRef,
        ));
    }
    let (fields, arrays): (Vec<_>, Vec<_>) = fields_and_arrays.into_iter().unzip();

    StructArray::try_new(fields.into(), arrays, nulls.finish())
        .map(|array| Arc::new(array) as ArrayRef)
        .map_err(|error| format!("failed to build Snowflake timestamp struct array: {error}"))
}

fn build_binary_array(index: usize, rows: &[Vec<JsonValue>]) -> Result<ArrayRef, String> {
    let mut builder = BinaryBuilder::new();
    for row in rows {
        let value = row.get(index).unwrap_or(&JsonValue::Null);
        if value.is_null() {
            builder.append_null();
        } else if let Some(raw) = value.as_str() {
            builder.append_value(decode_hex(raw)?);
        } else {
            return Err(format!("binary column value `{value}` is not a hex string"));
        }
    }
    Ok(Arc::new(builder.finish()))
}

fn json_value_to_string(value: &JsonValue) -> Result<String, String> {
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

fn json_value_to_i32(value: &JsonValue) -> Result<i32, String> {
    i32::try_from(json_value_to_i64(value)?).map_err(|_| format!("value `{value}` exceeds i32"))
}

fn json_value_to_i64(value: &JsonValue) -> Result<i64, String> {
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

fn json_value_to_f64(value: &JsonValue) -> Result<f64, String> {
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

fn json_value_to_bool(value: &JsonValue) -> Result<bool, String> {
    if let Some(value) = value.as_bool() {
        return Ok(value);
    }
    if let Some(value) = value.as_str() {
        return Ok(value.eq_ignore_ascii_case("true"));
    }
    Err(format!("value `{value}` is not a boolean"))
}

fn json_value_to_decimal(value: &JsonValue, scale: i64) -> Result<i128, String> {
    let factor = 10_i128
        .checked_pow(
            scale
                .try_into()
                .map_err(|_| format!("invalid decimal scale {scale}"))?,
        )
        .ok_or_else(|| format!("decimal scale {scale} is too large"))?;
    if let Some(value) = value.as_i64() {
        return Ok(i128::from(value) * factor);
    }
    if let Some(value) = value.as_u64() {
        return Ok(i128::from(value) * factor);
    }
    if let Some(value) = value.as_f64() {
        return Ok((value * factor as f64).round() as i128);
    }
    if let Some(value) = value.as_str() {
        return parse_decimal_string(value, scale, factor);
    }
    Err(format!("value `{value}` is not a decimal"))
}

fn parse_decimal_string(value: &str, scale: i64, factor: i128) -> Result<i128, String> {
    let (sign, unsigned) = value
        .trim()
        .strip_prefix('-')
        .map(|unsigned| (-1_i128, unsigned))
        .unwrap_or((1, value.trim()));
    let (whole, fractional) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    let whole = whole
        .parse::<i128>()
        .map_err(|error| format!("decimal `{value}` did not parse: {error}"))?;
    let mut fractional = fractional.to_owned();
    let scale = usize::try_from(scale).map_err(|_| format!("invalid decimal scale {scale}"))?;
    if fractional.len() > scale {
        fractional.truncate(scale);
    }
    while fractional.len() < scale {
        fractional.push('0');
    }
    let fractional = if fractional.is_empty() {
        0
    } else {
        fractional
            .parse::<i128>()
            .map_err(|error| format!("decimal `{value}` did not parse: {error}"))?
    };
    Ok(sign * (whole * factor + fractional))
}

fn decode_hex(value: &str) -> Result<Vec<u8>, String> {
    let value = value.trim();
    if !value.len().is_multiple_of(2) {
        return Err(format!("hex string `{value}` has odd length"));
    }
    (0..value.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&value[index..index + 2], 16)
                .map_err(|error| format!("hex string `{value}` did not parse: {error}"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use arrow_array::{
        Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array,
        Int64Array, StringArray,
    };
    use arrow_ipc::reader::StreamReader;

    use super::*;
    use crate::catalog::SnowflakeType;
    use serde_json::json;

    #[test]
    fn fixed_integer_column_uses_int64_with_snowflake_metadata() {
        let columns = vec![column(
            "ID",
            SnowflakeType::Fixed {
                precision: 18,
                scale: 0,
            },
            true,
        )];
        let batch =
            build_record_batch(&columns, &[vec![json!(7)], vec![JsonValue::Null]]).expect("batch");

        assert_eq!(batch.schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(
            batch.schema().field(0).metadata().get("logicalType"),
            Some(&"FIXED".to_owned())
        );
        assert_eq!(
            batch.schema().field(0).metadata().get("precision"),
            Some(&"18".to_owned())
        );
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 array");
        assert_eq!(7, array.value(0));
        assert!(array.is_null(1));
    }

    #[test]
    fn decimal_fixed_column_uses_decimal128() {
        let columns = vec![column(
            "AMOUNT",
            SnowflakeType::Fixed {
                precision: 38,
                scale: 2,
            },
            false,
        )];
        let batch = build_record_batch(&columns, &[vec![json!("12.34")]]).expect("decimal batch");

        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Decimal128(38, 2)
        );
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("decimal array");
        assert_eq!(1234, array.value(0));
    }

    #[test]
    fn text_column_uses_utf8_with_text_logical_type() {
        let columns = vec![column(
            "NAME",
            SnowflakeType::Text {
                char_length: 10,
                byte_length: 10,
            },
            true,
        )];
        let batch = build_record_batch(&columns, &[vec![json!("Alpha")]]).expect("text batch");

        assert_eq!(batch.schema().field(0).data_type(), &DataType::Utf8);
        assert_eq!(
            batch.schema().field(0).metadata().get("logicalType"),
            Some(&"TEXT".to_owned())
        );
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!("Alpha", array.value(0));
    }

    #[test]
    fn date_time_boolean_real_and_binary_mappings_are_typed() {
        let columns = vec![
            column("D", SnowflakeType::Date, true),
            column("T", SnowflakeType::Time { scale: 9 }, true),
            column("B", SnowflakeType::Boolean, true),
            column("R", SnowflakeType::Real, true),
            column("BIN", SnowflakeType::Binary { byte_length: 4 }, true),
        ];
        let batch = build_record_batch(
            &columns,
            &[vec![
                json!(20_000),
                json!(12),
                json!(true),
                json!(1.5),
                json!("0A0B"),
            ]],
        )
        .expect("typed batch");

        assert!(batch.column(0).as_any().is::<Date32Array>());
        assert!(batch.column(1).as_any().is::<Int64Array>());
        assert!(batch.column(2).as_any().is::<BooleanArray>());
        assert!(batch.column(3).as_any().is::<Float64Array>());
        assert!(batch.column(4).as_any().is::<BinaryArray>());
    }

    #[test]
    fn timestamp_ntz_column_uses_epoch_fraction_struct_and_handles_nulls() {
        let columns = vec![column("TS", SnowflakeType::TimestampNtz { scale: 6 }, true)];
        let batch = build_record_batch(
            &columns,
            &[vec![json!(1_234_567_i64)], vec![JsonValue::Null]],
        )
        .expect("timestamp batch");

        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Struct(timestamp_fields(false))
        );
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("timestamp struct");
        assert_eq!(1, array.null_count());
        let epoch = array
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("epoch array");
        let fraction = array
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("fraction array");
        assert_eq!(1, epoch.value(0));
        assert_eq!(234_567_000, fraction.value(0));
        assert!(array.is_null(1));
    }

    #[test]
    fn timestamp_tz_column_includes_biased_timezone() {
        let columns = vec![column(
            "TS_TZ",
            SnowflakeType::TimestampTz { scale: 6 },
            true,
        )];
        let batch =
            build_record_batch(&columns, &[vec![json!(1_000_000_i64)]]).expect("timestamp batch");
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("timestamp struct");
        let timezone = array
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("timezone array");
        assert_eq!(1440, timezone.value(0));
    }

    #[test]
    fn ipc_base64_decodes_as_stream() {
        let columns = vec![column(
            "NAME",
            SnowflakeType::Text {
                char_length: 5,
                byte_length: 5,
            },
            true,
        )];
        let encoded = encode_query_result(&columns, &[vec![json!("Alpha")]]).expect("encoded");
        let raw = BASE64
            .decode(encoded.rowset_base64)
            .expect("base64 decodes");
        let mut reader = StreamReader::try_new(std::io::Cursor::new(raw), None).expect("reader");
        let batch = reader
            .next()
            .expect("one batch")
            .expect("batch reads from stream");
        assert_eq!(1, batch.num_rows());
    }

    #[test]
    fn empty_result_without_columns_uses_empty_rowset_string() {
        let encoded = encode_query_result(&[], &[]).expect("encoded");
        assert_eq!("", encoded.rowset_base64);
        assert!(encoded.rowtype.is_empty());
    }

    fn column(name: &str, sf_type: SnowflakeType, nullable: bool) -> SnowflakeResultColumn {
        SnowflakeResultColumn {
            name: name.to_owned(),
            sf_type,
            nullable,
        }
    }
}
