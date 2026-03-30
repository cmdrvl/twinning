use thiserror::Error;

use crate::{
    catalog::TableCatalog,
    ir::{PredicateComparison, PredicateExpr, PredicateOperator, ScalarValue},
    kernel::value::KernelValue,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TruthValue {
    True,
    False,
    Unknown,
}

impl TruthValue {
    pub fn is_true(self) -> bool {
        matches!(self, Self::True)
    }

    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::False, _) | (_, Self::False) => Self::False,
            (Self::True, Self::True) => Self::True,
            _ => Self::Unknown,
        }
    }

    fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::True, _) | (_, Self::True) => Self::True,
            (Self::False, Self::False) => Self::False,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum PredicateError {
    #[error("predicate row for `{table}` has arity {actual}, expected {expected}")]
    RowArity {
        table: String,
        expected: usize,
        actual: usize,
    },
    #[error("predicate references unknown column `{column}` on `{table}`")]
    UnknownColumn { table: String, column: String },
    #[error("predicate operator `{operator}` expected {expected}, got {actual} value(s)")]
    OperatorArity {
        operator: String,
        expected: String,
        actual: usize,
    },
    #[error("predicate on `{table}.{column}` cannot compare `{row_kind}` to `{scalar_kind}`")]
    TypeMismatch {
        table: String,
        column: String,
        row_kind: String,
        scalar_kind: String,
    },
    #[error(
        "predicate operator `{operator}` is not supported for `{kind}` values on `{table}.{column}`"
    )]
    UnsupportedOperator {
        table: String,
        column: String,
        operator: String,
        kind: String,
    },
    #[error("predicate on `{table}.{column}` does not support kernel kind `{kind}`")]
    UnsupportedValueKind {
        table: String,
        column: String,
        kind: String,
    },
    #[error("predicate on `{table}.{column}` contains invalid numeric value `{value}`")]
    InvalidNumericValue {
        table: String,
        column: String,
        value: String,
    },
}

pub fn predicate_matches(
    table: &TableCatalog,
    row: &[KernelValue],
    predicate: &PredicateExpr,
) -> Result<bool, PredicateError> {
    Ok(evaluate_predicate(table, row, predicate)?.is_true())
}

pub fn evaluate_predicate(
    table: &TableCatalog,
    row: &[KernelValue],
    predicate: &PredicateExpr,
) -> Result<TruthValue, PredicateError> {
    validate_row_arity(table, row)?;

    match predicate {
        PredicateExpr::Comparison(comparison) => evaluate_comparison(table, row, comparison),
        PredicateExpr::Conjunction(comparisons) => {
            comparisons
                .iter()
                .try_fold(TruthValue::True, |result, comparison| {
                    evaluate_comparison(table, row, comparison).map(|value| result.and(value))
                })
        }
        PredicateExpr::Disjunction(comparisons) => {
            comparisons
                .iter()
                .try_fold(TruthValue::False, |result, comparison| {
                    evaluate_comparison(table, row, comparison).map(|value| result.or(value))
                })
        }
    }
}

pub fn evaluate_comparison(
    table: &TableCatalog,
    row: &[KernelValue],
    comparison: &PredicateComparison,
) -> Result<TruthValue, PredicateError> {
    validate_arity(comparison)?;
    let column = lookup_value(table, row, &comparison.column)?;

    match comparison.operator {
        PredicateOperator::IsNull => Ok(if column.is_null() {
            TruthValue::True
        } else {
            TruthValue::False
        }),
        PredicateOperator::InList => evaluate_in_list(
            table,
            &comparison.column,
            column,
            comparison.values.as_slice(),
        ),
        PredicateOperator::Between => evaluate_between(
            table,
            &comparison.column,
            column,
            comparison.values.as_slice(),
        ),
        operator => compare_kernel_to_scalar(
            table,
            &comparison.column,
            column,
            &comparison.values[0],
            operator,
        ),
    }
}

fn validate_row_arity(table: &TableCatalog, row: &[KernelValue]) -> Result<(), PredicateError> {
    if row.len() != table.columns.len() {
        return Err(PredicateError::RowArity {
            table: table.name.clone(),
            expected: table.columns.len(),
            actual: row.len(),
        });
    }

    Ok(())
}

fn validate_arity(comparison: &PredicateComparison) -> Result<(), PredicateError> {
    let expected = comparison.operator.value_arity();
    let actual = comparison.values.len();
    let valid = match expected {
        crate::ir::PredicateValueArity::Zero => actual == 0,
        crate::ir::PredicateValueArity::One => actual == 1,
        crate::ir::PredicateValueArity::OneOrMore => actual >= 1,
        crate::ir::PredicateValueArity::Two => actual == 2,
    };

    if valid {
        Ok(())
    } else {
        Err(PredicateError::OperatorArity {
            operator: predicate_operator_token(comparison.operator),
            expected: predicate_operator_expected_arity(comparison.operator),
            actual,
        })
    }
}

fn evaluate_in_list(
    table: &TableCatalog,
    column: &str,
    left: &KernelValue,
    values: &[ScalarValue],
) -> Result<TruthValue, PredicateError> {
    let mut saw_unknown = false;
    for value in values {
        match compare_kernel_to_scalar(table, column, left, value, PredicateOperator::Eq)? {
            TruthValue::True => return Ok(TruthValue::True),
            TruthValue::False => {}
            TruthValue::Unknown => saw_unknown = true,
        }
    }

    Ok(if saw_unknown {
        TruthValue::Unknown
    } else {
        TruthValue::False
    })
}

fn evaluate_between(
    table: &TableCatalog,
    column: &str,
    left: &KernelValue,
    values: &[ScalarValue],
) -> Result<TruthValue, PredicateError> {
    let lower = compare_kernel_to_scalar(table, column, left, &values[0], PredicateOperator::Gte)?;
    let upper = compare_kernel_to_scalar(table, column, left, &values[1], PredicateOperator::Lte)?;

    Ok(match (lower, upper) {
        (TruthValue::False, _) | (_, TruthValue::False) => TruthValue::False,
        (TruthValue::True, TruthValue::True) => TruthValue::True,
        _ => TruthValue::Unknown,
    })
}

fn compare_kernel_to_scalar(
    table: &TableCatalog,
    column: &str,
    left: &KernelValue,
    right: &ScalarValue,
    operator: PredicateOperator,
) -> Result<TruthValue, PredicateError> {
    let left = kernel_to_comparable(table, column, left)?;
    let right = scalar_to_comparable(right);

    let (Some(left), Some(right)) = (left, right) else {
        return Ok(TruthValue::Unknown);
    };

    match (left, right) {
        (ComparableValue::Boolean(left), ComparableValue::Boolean(right)) => match operator {
            PredicateOperator::Eq => Ok(bool_to_truth(left == right)),
            PredicateOperator::Neq => Ok(bool_to_truth(left != right)),
            _ => Err(PredicateError::UnsupportedOperator {
                table: table.name.clone(),
                column: column.to_owned(),
                operator: predicate_operator_token(operator),
                kind: String::from("boolean"),
            }),
        },
        (ComparableValue::Integer(left), ComparableValue::Integer(right)) => {
            Ok(compare_ord(left, right, operator))
        }
        (ComparableValue::Float(left), ComparableValue::Float(right)) => {
            Ok(compare_float(left, right, operator))
        }
        (ComparableValue::Text(left), ComparableValue::Text(right)) => {
            Ok(compare_ord(left, right, operator))
        }
        (ComparableValue::Integer(left), ComparableValue::Float(right)) => {
            Ok(compare_float(left as f64, right, operator))
        }
        (ComparableValue::Float(left), ComparableValue::Integer(right)) => {
            Ok(compare_float(left, right as f64, operator))
        }
        (left, right) => Err(PredicateError::TypeMismatch {
            table: table.name.clone(),
            column: column.to_owned(),
            row_kind: left.kind_token(),
            scalar_kind: right.kind_token(),
        }),
    }
}

fn lookup_value<'a>(
    table: &'a TableCatalog,
    row: &'a [KernelValue],
    column: &str,
) -> Result<&'a KernelValue, PredicateError> {
    let index = table
        .columns
        .iter()
        .position(|candidate| candidate.name == column)
        .ok_or_else(|| PredicateError::UnknownColumn {
            table: table.name.clone(),
            column: column.to_owned(),
        })?;

    Ok(&row[index])
}

fn kernel_to_comparable(
    table: &TableCatalog,
    column: &str,
    value: &KernelValue,
) -> Result<Option<ComparableValue>, PredicateError> {
    match value {
        KernelValue::Null => Ok(None),
        KernelValue::Boolean(value) => Ok(Some(ComparableValue::Boolean(*value))),
        KernelValue::Smallint(value) => Ok(Some(ComparableValue::Integer(i128::from(*value)))),
        KernelValue::Integer(value) => Ok(Some(ComparableValue::Integer(i128::from(*value)))),
        KernelValue::Bigint(value) => Ok(Some(ComparableValue::Integer(i128::from(*value)))),
        KernelValue::Numeric(value) => value
            .parse::<f64>()
            .map(ComparableValue::Float)
            .map(Some)
            .map_err(|_| PredicateError::InvalidNumericValue {
                table: table.name.clone(),
                column: column.to_owned(),
                value: value.clone(),
            }),
        KernelValue::Float(value) => Ok(Some(ComparableValue::Float(*value))),
        KernelValue::Text(value) | KernelValue::Date(value) | KernelValue::Timestamp(value) => {
            Ok(Some(ComparableValue::Text(value.clone())))
        }
        KernelValue::Bytes(_) | KernelValue::Json(_) | KernelValue::Array(_) => {
            Err(PredicateError::UnsupportedValueKind {
                table: table.name.clone(),
                column: column.to_owned(),
                kind: kernel_value_kind(value),
            })
        }
    }
}

fn scalar_to_comparable(value: &ScalarValue) -> Option<ComparableValue> {
    match value {
        ScalarValue::Null => None,
        ScalarValue::Boolean(value) => Some(ComparableValue::Boolean(*value)),
        ScalarValue::Integer(value) => Some(ComparableValue::Integer(i128::from(*value))),
        ScalarValue::Text(value) => Some(ComparableValue::Text(value.clone())),
    }
}

fn compare_ord<T>(left: T, right: T, operator: PredicateOperator) -> TruthValue
where
    T: Ord,
{
    bool_to_truth(match operator {
        PredicateOperator::Eq => left == right,
        PredicateOperator::Neq => left != right,
        PredicateOperator::Lt => left < right,
        PredicateOperator::Lte => left <= right,
        PredicateOperator::Gt => left > right,
        PredicateOperator::Gte => left >= right,
        _ => unreachable!("arithmetic comparisons only"),
    })
}

fn compare_float(left: f64, right: f64, operator: PredicateOperator) -> TruthValue {
    bool_to_truth(match operator {
        PredicateOperator::Eq => left == right,
        PredicateOperator::Neq => left != right,
        PredicateOperator::Lt => left < right,
        PredicateOperator::Lte => left <= right,
        PredicateOperator::Gt => left > right,
        PredicateOperator::Gte => left >= right,
        _ => unreachable!("arithmetic comparisons only"),
    })
}

fn bool_to_truth(value: bool) -> TruthValue {
    if value {
        TruthValue::True
    } else {
        TruthValue::False
    }
}

fn predicate_operator_expected_arity(operator: PredicateOperator) -> String {
    match operator.value_arity() {
        crate::ir::PredicateValueArity::Zero => String::from("zero values"),
        crate::ir::PredicateValueArity::One => String::from("one value"),
        crate::ir::PredicateValueArity::OneOrMore => String::from("one or more values"),
        crate::ir::PredicateValueArity::Two => String::from("two values"),
    }
}

fn predicate_operator_token(operator: PredicateOperator) -> String {
    match operator {
        PredicateOperator::Eq => String::from("eq"),
        PredicateOperator::Neq => String::from("neq"),
        PredicateOperator::Lt => String::from("lt"),
        PredicateOperator::Lte => String::from("lte"),
        PredicateOperator::Gt => String::from("gt"),
        PredicateOperator::Gte => String::from("gte"),
        PredicateOperator::IsNull => String::from("is_null"),
        PredicateOperator::InList => String::from("in_list"),
        PredicateOperator::Between => String::from("between"),
    }
}

fn kernel_value_kind(value: &KernelValue) -> String {
    match value {
        KernelValue::Null => String::from("null"),
        KernelValue::Bigint(_) => String::from("bigint"),
        KernelValue::Integer(_) => String::from("integer"),
        KernelValue::Smallint(_) => String::from("smallint"),
        KernelValue::Numeric(_) => String::from("numeric"),
        KernelValue::Float(_) => String::from("float"),
        KernelValue::Boolean(_) => String::from("boolean"),
        KernelValue::Timestamp(_) => String::from("timestamp"),
        KernelValue::Date(_) => String::from("date"),
        KernelValue::Bytes(_) => String::from("bytes"),
        KernelValue::Json(_) => String::from("json"),
        KernelValue::Text(_) => String::from("text"),
        KernelValue::Array(_) => String::from("array"),
    }
}

#[derive(Debug, Clone)]
enum ComparableValue {
    Boolean(bool),
    Integer(i128),
    Float(f64),
    Text(String),
}

impl ComparableValue {
    fn kind_token(&self) -> String {
        match self {
            Self::Boolean(_) => String::from("boolean"),
            Self::Integer(_) => String::from("integer"),
            Self::Float(_) => String::from("float"),
            Self::Text(_) => String::from("text"),
        }
    }
}
