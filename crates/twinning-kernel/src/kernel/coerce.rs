use chrono::{DateTime, NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::kernel::value::{KernelValue, ValueType};

pub const INVALID_TEXT_REPRESENTATION_SQLSTATE: &str = "22P02";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "input_kind", content = "value", rename_all = "snake_case")]
pub enum ClientInput {
    Null,
    Text(String),
    Boolean(bool),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
    Array(Vec<ClientInput>),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CoercionError {
    #[error("invalid input syntax for type {declared_type}: {input}")]
    InvalidSyntax {
        declared_type: ValueType,
        input: String,
    },
    #[error("unsupported client input kind `{input_kind}` for type {declared_type}")]
    UnsupportedInputKind {
        declared_type: ValueType,
        input_kind: &'static str,
    },
}

impl CoercionError {
    pub fn sqlstate(&self) -> &'static str {
        INVALID_TEXT_REPRESENTATION_SQLSTATE
    }
}

pub fn coerce_input(
    input: &ClientInput,
    declared_type: ValueType,
) -> Result<KernelValue, CoercionError> {
    match input {
        ClientInput::Null => Ok(KernelValue::Null),
        ClientInput::Text(text) => coerce_text_input(text, declared_type),
        ClientInput::Boolean(value) => match declared_type {
            ValueType::Boolean => Ok(KernelValue::Boolean(*value)),
            _ => Err(unsupported_input_kind(declared_type, input)),
        },
        ClientInput::Bytes(bytes) => match declared_type {
            ValueType::Bytes => Ok(KernelValue::Bytes(bytes.clone())),
            _ => Err(unsupported_input_kind(declared_type, input)),
        },
        ClientInput::Json(value) => match declared_type {
            ValueType::Json => Ok(KernelValue::Json(value.clone())),
            _ => Err(unsupported_input_kind(declared_type, input)),
        },
        ClientInput::Array(values) => match declared_type {
            ValueType::Array => values
                .iter()
                .map(coerce_untyped_array_member)
                .collect::<Result<Vec<_>, _>>()
                .map(KernelValue::Array),
            _ => Err(unsupported_input_kind(declared_type, input)),
        },
    }
}

fn coerce_text_input(text: &str, declared_type: ValueType) -> Result<KernelValue, CoercionError> {
    let trimmed = text.trim();

    match declared_type {
        ValueType::Bigint => trimmed
            .parse::<i64>()
            .map(KernelValue::Bigint)
            .map_err(|_| invalid_syntax(declared_type, text)),
        ValueType::Integer => trimmed
            .parse::<i32>()
            .map(KernelValue::Integer)
            .map_err(|_| invalid_syntax(declared_type, text)),
        ValueType::Smallint => trimmed
            .parse::<i16>()
            .map(KernelValue::Smallint)
            .map_err(|_| invalid_syntax(declared_type, text)),
        ValueType::Numeric => {
            if parse_numeric_literal(trimmed) {
                Ok(KernelValue::Numeric(trimmed.to_owned()))
            } else {
                Err(invalid_syntax(declared_type, text))
            }
        }
        ValueType::Float => trimmed
            .parse::<f64>()
            .map(KernelValue::Float)
            .map_err(|_| invalid_syntax(declared_type, text)),
        ValueType::Boolean => parse_boolean_literal(trimmed)
            .map(KernelValue::Boolean)
            .ok_or_else(|| invalid_syntax(declared_type, text)),
        ValueType::Timestamp => parse_timestamp_literal(trimmed)
            .map(|_| KernelValue::Timestamp(trimmed.to_owned()))
            .ok_or_else(|| invalid_syntax(declared_type, text)),
        ValueType::Date => NaiveDate::parse_from_str(trimmed, "%Y-%m-%d")
            .map(|_| KernelValue::Date(trimmed.to_owned()))
            .map_err(|_| invalid_syntax(declared_type, text)),
        ValueType::Bytes => parse_bytea_hex(trimmed)
            .map(KernelValue::Bytes)
            .ok_or_else(|| invalid_syntax(declared_type, text)),
        ValueType::Json => serde_json::from_str(trimmed)
            .map(KernelValue::Json)
            .map_err(|_| invalid_syntax(declared_type, text)),
        ValueType::Text => Ok(KernelValue::Text(text.to_owned())),
        ValueType::Array => Err(invalid_syntax(declared_type, text)),
    }
}

fn coerce_untyped_array_member(input: &ClientInput) -> Result<KernelValue, CoercionError> {
    match input {
        ClientInput::Null => Ok(KernelValue::Null),
        ClientInput::Text(text) => Ok(KernelValue::Text(text.clone())),
        ClientInput::Boolean(value) => Ok(KernelValue::Boolean(*value)),
        ClientInput::Bytes(bytes) => Ok(KernelValue::Bytes(bytes.clone())),
        ClientInput::Json(value) => Ok(KernelValue::Json(value.clone())),
        ClientInput::Array(values) => values
            .iter()
            .map(coerce_untyped_array_member)
            .collect::<Result<Vec<_>, _>>()
            .map(KernelValue::Array),
    }
}

fn parse_boolean_literal(text: &str) -> Option<bool> {
    match text.to_ascii_lowercase().as_str() {
        "true" | "t" | "1" | "yes" | "y" | "on" => Some(true),
        "false" | "f" | "0" | "no" | "n" | "off" => Some(false),
        _ => None,
    }
}

fn parse_timestamp_literal(text: &str) -> Option<()> {
    DateTime::parse_from_rfc3339(text)
        .map(|_| ())
        .or_else(|_| NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S").map(|_| ()))
        .or_else(|_| NaiveDateTime::parse_from_str(text, "%Y-%m-%dT%H:%M:%S").map(|_| ()))
        .ok()
}

fn parse_bytea_hex(text: &str) -> Option<Vec<u8>> {
    let hex = text
        .strip_prefix("\\x")
        .or_else(|| text.strip_prefix("\\X"))?;

    if hex.len() % 2 != 0 {
        return None;
    }

    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            std::str::from_utf8(pair)
                .ok()
                .and_then(|byte| u8::from_str_radix(byte, 16).ok())
        })
        .collect()
}

fn parse_numeric_literal(text: &str) -> bool {
    if text.is_empty() {
        return false;
    }

    let Some((mantissa, exponent)) = split_exponent(text) else {
        return false;
    };

    valid_decimal_mantissa(mantissa) && exponent.is_none_or(valid_signed_digits)
}

fn split_exponent(text: &str) -> Option<(&str, Option<&str>)> {
    let exponent_index = text.find(['e', 'E']);
    match exponent_index {
        Some(index) => {
            let (mantissa, exponent_with_marker) = text.split_at(index);
            let exponent = exponent_with_marker.get(1..)?;
            Some((mantissa, Some(exponent)))
        }
        None => Some((text, None)),
    }
}

fn valid_decimal_mantissa(text: &str) -> bool {
    let body = text
        .strip_prefix('+')
        .or_else(|| text.strip_prefix('-'))
        .unwrap_or(text);

    if body.is_empty() {
        return false;
    }

    let dot_count = body.chars().filter(|character| *character == '.').count();
    if dot_count > 1 {
        return false;
    }

    let digits_only = body
        .chars()
        .filter(|character| *character != '.')
        .collect::<String>();
    !digits_only.is_empty()
        && digits_only
            .chars()
            .all(|character| character.is_ascii_digit())
}

fn valid_signed_digits(text: &str) -> bool {
    let body = text
        .strip_prefix('+')
        .or_else(|| text.strip_prefix('-'))
        .unwrap_or(text);

    !body.is_empty() && body.chars().all(|character| character.is_ascii_digit())
}

fn invalid_syntax(declared_type: ValueType, input: &str) -> CoercionError {
    CoercionError::InvalidSyntax {
        declared_type,
        input: input.to_owned(),
    }
}

fn unsupported_input_kind(declared_type: ValueType, input: &ClientInput) -> CoercionError {
    CoercionError::UnsupportedInputKind {
        declared_type,
        input_kind: input.kind_name(),
    }
}

impl ClientInput {
    fn kind_name(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Text(_) => "text",
            Self::Boolean(_) => "boolean",
            Self::Bytes(_) => "bytes",
            Self::Json(_) => "json",
            Self::Array(_) => "array",
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::kernel::value::ValueType;

    use super::{ClientInput, CoercionError, INVALID_TEXT_REPRESENTATION_SQLSTATE, coerce_input};

    #[test]
    fn text_inputs_coerce_into_declared_scalar_values() {
        let cases = [
            (
                ClientInput::Text(String::from("9223372036854775807")),
                ValueType::Bigint,
                json!({ "kind": "bigint", "value": 9223372036854775807i64 }),
            ),
            (
                ClientInput::Text(String::from("42")),
                ValueType::Integer,
                json!({ "kind": "integer", "value": 42 }),
            ),
            (
                ClientInput::Text(String::from("-7")),
                ValueType::Smallint,
                json!({ "kind": "smallint", "value": -7 }),
            ),
            (
                ClientInput::Text(String::from("10.50")),
                ValueType::Numeric,
                json!({ "kind": "numeric", "value": "10.50" }),
            ),
            (
                ClientInput::Text(String::from("7.25")),
                ValueType::Float,
                json!({ "kind": "float", "value": 7.25 }),
            ),
            (
                ClientInput::Text(String::from("YES")),
                ValueType::Boolean,
                json!({ "kind": "boolean", "value": true }),
            ),
            (
                ClientInput::Text(String::from("2026-03-30 18:24:00")),
                ValueType::Timestamp,
                json!({ "kind": "timestamp", "value": "2026-03-30 18:24:00" }),
            ),
            (
                ClientInput::Text(String::from("2026-03-30")),
                ValueType::Date,
                json!({ "kind": "date", "value": "2026-03-30" }),
            ),
            (
                ClientInput::Text(String::from("\\xdeadbeef")),
                ValueType::Bytes,
                json!({ "kind": "bytes", "value": [222, 173, 190, 239] }),
            ),
            (
                ClientInput::Text(String::from("{\"deal_id\":\"deal-1\"}")),
                ValueType::Json,
                json!({ "kind": "json", "value": { "deal_id": "deal-1" } }),
            ),
            (
                ClientInput::Text(String::from("deal-1")),
                ValueType::Text,
                json!({ "kind": "text", "value": "deal-1" }),
            ),
        ];

        for (input, declared_type, expected) in cases {
            let coerced = coerce_input(&input, declared_type).expect("coerce input");
            assert_eq!(
                serde_json::to_value(&coerced).expect("serialize value"),
                expected
            );
            assert!(coerced.fits_declared_type(declared_type));
        }
    }

    #[test]
    fn null_and_typed_inputs_follow_declared_kernel_types() {
        assert_eq!(
            serde_json::to_value(
                coerce_input(&ClientInput::Null, ValueType::Numeric).expect("null")
            )
            .expect("serialize null"),
            json!({ "kind": "null" })
        );

        assert_eq!(
            serde_json::to_value(
                coerce_input(&ClientInput::Bytes(vec![0xde, 0xad]), ValueType::Bytes)
                    .expect("bytes")
            )
            .expect("serialize bytes"),
            json!({ "kind": "bytes", "value": [222, 173] })
        );

        assert_eq!(
            serde_json::to_value(
                coerce_input(
                    &ClientInput::Json(json!({ "deal_id": "deal-1" })),
                    ValueType::Json
                )
                .expect("json")
            )
            .expect("serialize json"),
            json!({ "kind": "json", "value": { "deal_id": "deal-1" } })
        );

        assert_eq!(
            serde_json::to_value(
                coerce_input(
                    &ClientInput::Array(vec![
                        ClientInput::Text(String::from("deal-1")),
                        ClientInput::Boolean(true),
                        ClientInput::Null,
                    ]),
                    ValueType::Array,
                )
                .expect("array")
            )
            .expect("serialize array"),
            json!({
                "kind": "array",
                "value": [
                    { "kind": "text", "value": "deal-1" },
                    { "kind": "boolean", "value": true },
                    { "kind": "null" }
                ]
            })
        );
    }

    #[test]
    fn bad_coercions_map_to_sqlstate_22p02() {
        let cases = [
            (
                ClientInput::Text(String::from("N/A")),
                ValueType::Numeric,
                "invalid input syntax for type numeric: N/A",
            ),
            (
                ClientInput::Text(String::from("maybe")),
                ValueType::Boolean,
                "invalid input syntax for type boolean: maybe",
            ),
            (
                ClientInput::Text(String::from("2026-02-30")),
                ValueType::Date,
                "invalid input syntax for type date: 2026-02-30",
            ),
            (
                ClientInput::Text(String::from("\\xabc")),
                ValueType::Bytes,
                "invalid input syntax for type bytes: \\xabc",
            ),
        ];

        for (input, declared_type, expected_message) in cases {
            let error = coerce_input(&input, declared_type).expect_err("coercion should fail");
            assert_eq!(error.sqlstate(), INVALID_TEXT_REPRESENTATION_SQLSTATE);
            assert_eq!(error.to_string(), expected_message);
        }
    }

    #[test]
    fn unsupported_input_kinds_still_report_invalid_text_representation() {
        let error = coerce_input(&ClientInput::Bytes(vec![0xde, 0xad]), ValueType::Integer)
            .expect_err("bytes should not coerce into integer");

        assert_eq!(
            error,
            CoercionError::UnsupportedInputKind {
                declared_type: ValueType::Integer,
                input_kind: "bytes",
            }
        );
        assert_eq!(error.sqlstate(), INVALID_TEXT_REPRESENTATION_SQLSTATE);
    }

    #[test]
    fn integer_overflow_refuses_cleanly() {
        let error = coerce_input(
            &ClientInput::Text(String::from("2147483648")),
            ValueType::Integer,
        )
        .expect_err("overflow should refuse");

        assert_eq!(
            error,
            CoercionError::InvalidSyntax {
                declared_type: ValueType::Integer,
                input: String::from("2147483648"),
            }
        );
        assert_eq!(error.sqlstate(), INVALID_TEXT_REPRESENTATION_SQLSTATE);
    }
}
