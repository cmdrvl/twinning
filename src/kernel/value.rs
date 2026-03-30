use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueType {
    Array,
    Bigint,
    Integer,
    Smallint,
    Numeric,
    Float,
    Boolean,
    Timestamp,
    Date,
    Bytes,
    Json,
    Text,
}

impl ValueType {
    pub fn from_normalized_catalog_type(normalized_type: &str) -> Result<Self, ValueTypeError> {
        match normalized_type {
            "array" => Ok(Self::Array),
            "bigint" => Ok(Self::Bigint),
            "integer" => Ok(Self::Integer),
            "smallint" => Ok(Self::Smallint),
            "numeric" => Ok(Self::Numeric),
            "float" => Ok(Self::Float),
            "boolean" => Ok(Self::Boolean),
            "timestamp" => Ok(Self::Timestamp),
            "date" => Ok(Self::Date),
            "bytes" => Ok(Self::Bytes),
            "json" => Ok(Self::Json),
            "text" => Ok(Self::Text),
            other => Err(ValueTypeError::UnsupportedNormalizedType(other.to_owned())),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Array => "array",
            Self::Bigint => "bigint",
            Self::Integer => "integer",
            Self::Smallint => "smallint",
            Self::Numeric => "numeric",
            Self::Float => "float",
            Self::Boolean => "boolean",
            Self::Timestamp => "timestamp",
            Self::Date => "date",
            Self::Bytes => "bytes",
            Self::Json => "json",
            Self::Text => "text",
        }
    }
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum KernelValue {
    Null,
    Bigint(i64),
    Integer(i32),
    Smallint(i16),
    Numeric(String),
    Float(f64),
    Boolean(bool),
    Timestamp(String),
    Date(String),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
    Text(String),
    Array(Vec<KernelValue>),
}

impl KernelValue {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn value_type(&self) -> Option<ValueType> {
        match self {
            Self::Null => None,
            Self::Bigint(_) => Some(ValueType::Bigint),
            Self::Integer(_) => Some(ValueType::Integer),
            Self::Smallint(_) => Some(ValueType::Smallint),
            Self::Numeric(_) => Some(ValueType::Numeric),
            Self::Float(_) => Some(ValueType::Float),
            Self::Boolean(_) => Some(ValueType::Boolean),
            Self::Timestamp(_) => Some(ValueType::Timestamp),
            Self::Date(_) => Some(ValueType::Date),
            Self::Bytes(_) => Some(ValueType::Bytes),
            Self::Json(_) => Some(ValueType::Json),
            Self::Text(_) => Some(ValueType::Text),
            Self::Array(_) => Some(ValueType::Array),
        }
    }

    pub fn fits_declared_type(&self, declared_type: ValueType) -> bool {
        self.is_null() || self.value_type() == Some(declared_type)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValueTypeError {
    UnsupportedNormalizedType(String),
}

impl fmt::Display for ValueTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedNormalizedType(normalized_type) => write!(
                f,
                "normalized catalog type `{normalized_type}` is outside the declared kernel value subset"
            ),
        }
    }
}

impl std::error::Error for ValueTypeError {}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{KernelValue, ValueType, ValueTypeError};

    #[test]
    fn normalized_catalog_types_map_into_declared_value_types() {
        let mapped = [
            ("array", ValueType::Array),
            ("bigint", ValueType::Bigint),
            ("integer", ValueType::Integer),
            ("smallint", ValueType::Smallint),
            ("numeric", ValueType::Numeric),
            ("float", ValueType::Float),
            ("boolean", ValueType::Boolean),
            ("timestamp", ValueType::Timestamp),
            ("date", ValueType::Date),
            ("bytes", ValueType::Bytes),
            ("json", ValueType::Json),
            ("text", ValueType::Text),
        ];

        for (normalized, expected) in mapped {
            assert_eq!(
                ValueType::from_normalized_catalog_type(normalized).expect("map value type"),
                expected
            );
            assert_eq!(expected.as_str(), normalized);
        }
    }

    #[test]
    fn unsupported_catalog_types_refuse_cleanly() {
        assert_eq!(
            ValueType::from_normalized_catalog_type("uuid"),
            Err(ValueTypeError::UnsupportedNormalizedType(String::from(
                "uuid"
            )))
        );
    }

    #[test]
    fn kernel_values_report_their_runtime_type() {
        let values = [
            (KernelValue::Bigint(7), Some(ValueType::Bigint)),
            (KernelValue::Integer(7), Some(ValueType::Integer)),
            (KernelValue::Smallint(7), Some(ValueType::Smallint)),
            (
                KernelValue::Numeric(String::from("7.25")),
                Some(ValueType::Numeric),
            ),
            (KernelValue::Float(7.25), Some(ValueType::Float)),
            (KernelValue::Boolean(true), Some(ValueType::Boolean)),
            (
                KernelValue::Timestamp(String::from("2026-03-30T18:24:00Z")),
                Some(ValueType::Timestamp),
            ),
            (
                KernelValue::Date(String::from("2026-03-30")),
                Some(ValueType::Date),
            ),
            (KernelValue::Bytes(vec![0xde, 0xad]), Some(ValueType::Bytes)),
            (
                KernelValue::Json(json!({ "deal_id": "deal-1" })),
                Some(ValueType::Json),
            ),
            (
                KernelValue::Text(String::from("deal-1")),
                Some(ValueType::Text),
            ),
            (
                KernelValue::Array(vec![KernelValue::Text(String::from("deal-1"))]),
                Some(ValueType::Array),
            ),
            (KernelValue::Null, None),
        ];

        for (value, expected_type) in values {
            assert_eq!(value.value_type(), expected_type);
        }
    }

    #[test]
    fn null_fits_any_declared_type_but_non_null_values_remain_exact() {
        let null = KernelValue::Null;
        assert!(null.fits_declared_type(ValueType::Text));
        assert!(null.fits_declared_type(ValueType::Numeric));

        let text = KernelValue::Text(String::from("deal-1"));
        assert!(text.fits_declared_type(ValueType::Text));
        assert!(!text.fits_declared_type(ValueType::Integer));

        let numeric = KernelValue::Numeric(String::from("10.50"));
        assert!(numeric.fits_declared_type(ValueType::Numeric));
        assert!(!numeric.fits_declared_type(ValueType::Float));
    }

    #[test]
    fn kernel_values_serialize_with_explicit_kind_tags() {
        let value = KernelValue::Array(vec![
            KernelValue::Text(String::from("deal-1")),
            KernelValue::Null,
        ]);

        assert_eq!(
            serde_json::to_value(value).expect("serialize kernel value"),
            json!({
                "kind": "array",
                "value": [
                    {
                        "kind": "text",
                        "value": "deal-1"
                    },
                    {
                        "kind": "null"
                    }
                ]
            })
        );
    }
}
