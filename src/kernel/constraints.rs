use std::collections::BTreeMap;

use sqlparser::{
    ast::{BinaryOperator, Expr, Ident, UnaryOperator, Value as SqlValue},
    dialect::PostgreSqlDialect,
    parser::Parser,
};
use thiserror::Error;

use crate::{
    catalog::{CheckConstraint, TableCatalog},
    kernel::{storage::TableStorage, value::KernelValue},
};

pub const NOT_NULL_VIOLATION_SQLSTATE: &str = "23502";
pub const FOREIGN_KEY_VIOLATION_SQLSTATE: &str = "23503";
pub const UNIQUE_VIOLATION_SQLSTATE: &str = "23505";
pub const CHECK_VIOLATION_SQLSTATE: &str = "23514";
const INTERNAL_ERROR_SQLSTATE: &str = "XX000";

pub fn enforce_insert_constraints<'a, F>(
    table: &TableCatalog,
    storage: &TableStorage,
    row: &[KernelValue],
    lookup_table: F,
) -> Result<(), ConstraintError>
where
    F: Fn(&str) -> Option<&'a TableStorage>,
{
    if row.len() != table.columns.len() {
        return Err(ConstraintError::RowArity {
            table: table.name.clone(),
            expected: table.columns.len(),
            actual: row.len(),
        });
    }

    enforce_not_null(table, row)?;
    enforce_primary_key_uniqueness(table, storage, row)?;
    enforce_unique_constraints(table, storage, row)?;
    enforce_foreign_keys(table, row, lookup_table)?;
    enforce_checks(table, row)?;
    Ok(())
}

#[derive(Debug, Error)]
pub enum ConstraintError {
    #[error("table `{table}` row arity {actual} does not match declared column count {expected}")]
    RowArity {
        table: String,
        expected: usize,
        actual: usize,
    },
    #[error("table `{table}` column `{column}` violates NOT NULL")]
    NotNullViolation { table: String, column: String },
    #[error("table `{table}` violates unique surface {columns:?}")]
    UniqueViolation {
        table: String,
        columns: Vec<String>,
        name: Option<String>,
    },
    #[error(
        "table `{table}` foreign key {columns:?} references missing row in `{foreign_table}` {referred_columns:?}"
    )]
    ForeignKeyViolation {
        table: String,
        columns: Vec<String>,
        foreign_table: String,
        referred_columns: Vec<String>,
        name: Option<String>,
    },
    #[error("table `{table}` check constraint failed: {expression}")]
    CheckViolation {
        table: String,
        expression: String,
        name: Option<String>,
    },
    #[error("table `{table}` references unknown column `{column}`")]
    UnknownColumn { table: String, column: String },
    #[error("table `{table}` references unknown related table `{foreign_table}`")]
    UnknownRelatedTable {
        table: String,
        foreign_table: String,
    },
    #[error("unsupported check expression on `{table}`: {expression}")]
    UnsupportedCheckExpression { table: String, expression: String },
}

impl ConstraintError {
    pub fn sqlstate(&self) -> &'static str {
        match self {
            Self::NotNullViolation { .. } => NOT_NULL_VIOLATION_SQLSTATE,
            Self::ForeignKeyViolation { .. } => FOREIGN_KEY_VIOLATION_SQLSTATE,
            Self::UniqueViolation { .. } => UNIQUE_VIOLATION_SQLSTATE,
            Self::CheckViolation { .. } => CHECK_VIOLATION_SQLSTATE,
            Self::RowArity { .. }
            | Self::UnknownColumn { .. }
            | Self::UnknownRelatedTable { .. }
            | Self::UnsupportedCheckExpression { .. } => INTERNAL_ERROR_SQLSTATE,
        }
    }
}

fn enforce_not_null(table: &TableCatalog, row: &[KernelValue]) -> Result<(), ConstraintError> {
    for (column, value) in table.columns.iter().zip(row.iter()) {
        let required = !column.nullable
            || table
                .primary_key
                .as_ref()
                .is_some_and(|key| key.columns.contains(&column.name));
        if required && value.is_null() {
            return Err(ConstraintError::NotNullViolation {
                table: table.name.clone(),
                column: column.name.clone(),
            });
        }
    }

    Ok(())
}

fn enforce_primary_key_uniqueness(
    table: &TableCatalog,
    storage: &TableStorage,
    row: &[KernelValue],
) -> Result<(), ConstraintError> {
    let Some(primary_key) = &table.primary_key else {
        return Ok(());
    };

    let values = lookup_values(table, row, &primary_key.columns)?;
    if values.iter().any(KernelValue::is_null) {
        return Ok(());
    }

    if storage.lookup_primary_key(&values).ok().flatten().is_some() {
        return Err(ConstraintError::UniqueViolation {
            table: table.name.clone(),
            columns: primary_key.columns.clone(),
            name: primary_key.name.clone(),
        });
    }

    Ok(())
}

fn enforce_unique_constraints(
    table: &TableCatalog,
    storage: &TableStorage,
    row: &[KernelValue],
) -> Result<(), ConstraintError> {
    for unique in &table.unique_constraints {
        let values = lookup_values(table, row, &unique.columns)?;
        if values.iter().any(KernelValue::is_null) {
            continue;
        }

        let columns = unique
            .columns
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        if storage
            .lookup_unique(&columns, &values)
            .ok()
            .flatten()
            .is_some()
        {
            return Err(ConstraintError::UniqueViolation {
                table: table.name.clone(),
                columns: unique.columns.clone(),
                name: unique.name.clone(),
            });
        }
    }

    Ok(())
}

fn enforce_foreign_keys<'a, F>(
    table: &TableCatalog,
    row: &[KernelValue],
    lookup_table: F,
) -> Result<(), ConstraintError>
where
    F: Fn(&str) -> Option<&'a TableStorage>,
{
    for foreign_key in &table.foreign_keys {
        let values = lookup_values(table, row, &foreign_key.columns)?;
        if values.iter().any(KernelValue::is_null) {
            continue;
        }

        let parent = lookup_table(&foreign_key.foreign_table).ok_or_else(|| {
            ConstraintError::UnknownRelatedTable {
                table: table.name.clone(),
                foreign_table: foreign_key.foreign_table.clone(),
            }
        })?;

        let parent_found = if parent
            .primary_key_surface()
            .is_some_and(|surface| surface.columns == foreign_key.referred_columns)
        {
            parent.lookup_primary_key(&values).ok().flatten().is_some()
        } else if parent
            .unique_surfaces()
            .iter()
            .any(|surface| surface.columns == foreign_key.referred_columns)
        {
            let columns = foreign_key
                .referred_columns
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            parent
                .lookup_unique(&columns, &values)
                .ok()
                .flatten()
                .is_some()
        } else {
            false
        };

        if !parent_found {
            return Err(ConstraintError::ForeignKeyViolation {
                table: table.name.clone(),
                columns: foreign_key.columns.clone(),
                foreign_table: foreign_key.foreign_table.clone(),
                referred_columns: foreign_key.referred_columns.clone(),
                name: foreign_key.name.clone(),
            });
        }
    }

    Ok(())
}

fn enforce_checks(table: &TableCatalog, row: &[KernelValue]) -> Result<(), ConstraintError> {
    let row_values = table
        .columns
        .iter()
        .zip(row.iter())
        .map(|(column, value)| (column.name.as_str(), value))
        .collect::<BTreeMap<_, _>>();

    for check in &table.checks {
        let expr = parse_check_expression(table, check)?;
        let satisfied = evaluate_bool_expr(table, &row_values, &expr)?;
        if satisfied == Some(false) {
            return Err(ConstraintError::CheckViolation {
                table: table.name.clone(),
                expression: check.expression.clone(),
                name: check.name.clone(),
            });
        }
    }

    Ok(())
}

fn parse_check_expression(
    table: &TableCatalog,
    check: &CheckConstraint,
) -> Result<Expr, ConstraintError> {
    let dialect = PostgreSqlDialect {};
    let mut parser = Parser::new(&dialect)
        .try_with_sql(&check.expression)
        .map_err(|_| ConstraintError::UnsupportedCheckExpression {
            table: table.name.clone(),
            expression: check.expression.clone(),
        })?;
    parser
        .parse_expr()
        .map_err(|_| ConstraintError::UnsupportedCheckExpression {
            table: table.name.clone(),
            expression: check.expression.clone(),
        })
}

fn lookup_values(
    table: &TableCatalog,
    row: &[KernelValue],
    columns: &[String],
) -> Result<Vec<KernelValue>, ConstraintError> {
    columns
        .iter()
        .map(|column| lookup_value(table, row, column).cloned())
        .collect()
}

fn lookup_value<'a>(
    table: &'a TableCatalog,
    row: &'a [KernelValue],
    column: &str,
) -> Result<&'a KernelValue, ConstraintError> {
    let index = table
        .columns
        .iter()
        .position(|candidate| candidate.name == column)
        .ok_or_else(|| ConstraintError::UnknownColumn {
            table: table.name.clone(),
            column: column.to_owned(),
        })?;
    Ok(&row[index])
}

fn evaluate_bool_expr(
    table: &TableCatalog,
    row: &BTreeMap<&str, &KernelValue>,
    expr: &Expr,
) -> Result<Option<bool>, ConstraintError> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(and3(
                evaluate_bool_expr(table, row, left)?,
                evaluate_bool_expr(table, row, right)?,
            )),
            BinaryOperator::Or => Ok(or3(
                evaluate_bool_expr(table, row, left)?,
                evaluate_bool_expr(table, row, right)?,
            )),
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq => {
                compare_expr_values(table, row, left, right, op.clone(), &expr.to_string())
            }
            _ => Err(ConstraintError::UnsupportedCheckExpression {
                table: table.name.clone(),
                expression: expr.to_string(),
            }),
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let target = evaluate_scalar_expr(table, row, expr, &expr.to_string())?;
            let Some(target) = target else {
                return Ok(None);
            };

            let mut saw_null = false;
            for item in list {
                match compare_values(
                    Some(&target),
                    evaluate_scalar_expr(table, row, item, &expr.to_string())?.as_ref(),
                    BinaryOperator::Eq,
                    &expr.to_string(),
                    table,
                )? {
                    Some(true) => return Ok(Some(!negated)),
                    Some(false) => {}
                    None => saw_null = true,
                }
            }

            if saw_null {
                Ok(None)
            } else {
                Ok(Some(*negated))
            }
        }
        Expr::IsNull(expr) => Ok(Some(
            evaluate_scalar_expr(table, row, expr, &expr.to_string())?.is_none(),
        )),
        Expr::IsNotNull(expr) => Ok(Some(
            evaluate_scalar_expr(table, row, expr, &expr.to_string())?.is_some(),
        )),
        Expr::Nested(expr) => evaluate_bool_expr(table, row, expr),
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => Ok(evaluate_bool_expr(table, row, expr)?.map(|value| !value)),
        _ => Err(ConstraintError::UnsupportedCheckExpression {
            table: table.name.clone(),
            expression: expr.to_string(),
        }),
    }
}

fn compare_expr_values(
    table: &TableCatalog,
    row: &BTreeMap<&str, &KernelValue>,
    left: &Expr,
    right: &Expr,
    op: BinaryOperator,
    context: &str,
) -> Result<Option<bool>, ConstraintError> {
    let left = evaluate_scalar_expr(table, row, left, context)?;
    let right = evaluate_scalar_expr(table, row, right, context)?;
    compare_values(left.as_ref(), right.as_ref(), op, context, table)
}

fn compare_values(
    left: Option<&ComparableValue>,
    right: Option<&ComparableValue>,
    op: BinaryOperator,
    context: &str,
    table: &TableCatalog,
) -> Result<Option<bool>, ConstraintError> {
    let (Some(left), Some(right)) = (left, right) else {
        return Ok(None);
    };

    match (left, right) {
        (ComparableValue::Boolean(left), ComparableValue::Boolean(right)) => match op {
            BinaryOperator::Eq => Ok(Some(left == right)),
            BinaryOperator::NotEq => Ok(Some(left != right)),
            _ => Err(ConstraintError::UnsupportedCheckExpression {
                table: table.name.clone(),
                expression: context.to_owned(),
            }),
        },
        (ComparableValue::Integer(left), ComparableValue::Integer(right)) => Ok(Some(match op {
            BinaryOperator::Eq => left == right,
            BinaryOperator::NotEq => left != right,
            BinaryOperator::Gt => left > right,
            BinaryOperator::GtEq => left >= right,
            BinaryOperator::Lt => left < right,
            BinaryOperator::LtEq => left <= right,
            _ => unreachable!(),
        })),
        (ComparableValue::Float(left), ComparableValue::Float(right)) => Ok(Some(match op {
            BinaryOperator::Eq => left == right,
            BinaryOperator::NotEq => left != right,
            BinaryOperator::Gt => left > right,
            BinaryOperator::GtEq => left >= right,
            BinaryOperator::Lt => left < right,
            BinaryOperator::LtEq => left <= right,
            _ => unreachable!(),
        })),
        (ComparableValue::Text(left), ComparableValue::Text(right)) => Ok(Some(match op {
            BinaryOperator::Eq => left == right,
            BinaryOperator::NotEq => left != right,
            BinaryOperator::Gt => left > right,
            BinaryOperator::GtEq => left >= right,
            BinaryOperator::Lt => left < right,
            BinaryOperator::LtEq => left <= right,
            _ => unreachable!(),
        })),
        (ComparableValue::Integer(left), ComparableValue::Float(right)) => compare_values(
            Some(&ComparableValue::Float(*left as f64)),
            Some(&ComparableValue::Float(*right)),
            op,
            context,
            table,
        ),
        (ComparableValue::Float(left), ComparableValue::Integer(right)) => compare_values(
            Some(&ComparableValue::Float(*left)),
            Some(&ComparableValue::Float(*right as f64)),
            op,
            context,
            table,
        ),
        _ => Err(ConstraintError::UnsupportedCheckExpression {
            table: table.name.clone(),
            expression: context.to_owned(),
        }),
    }
}

fn evaluate_scalar_expr(
    table: &TableCatalog,
    row: &BTreeMap<&str, &KernelValue>,
    expr: &Expr,
    context: &str,
) -> Result<Option<ComparableValue>, ConstraintError> {
    match expr {
        Expr::Identifier(ident) => lookup_row_value(table, row, ident),
        Expr::CompoundIdentifier(parts) => parts
            .last()
            .ok_or_else(|| ConstraintError::UnsupportedCheckExpression {
                table: table.name.clone(),
                expression: context.to_owned(),
            })
            .and_then(|ident| lookup_row_value(table, row, ident)),
        Expr::Value(value) => scalar_from_sql_value(value, table, context),
        Expr::Nested(expr) => evaluate_scalar_expr(table, row, expr, context),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => evaluate_scalar_expr(table, row, expr, context).and_then(|value| match value {
            Some(ComparableValue::Integer(value)) => Ok(Some(ComparableValue::Integer(-value))),
            Some(ComparableValue::Float(value)) => Ok(Some(ComparableValue::Float(-value))),
            Some(_) => Err(ConstraintError::UnsupportedCheckExpression {
                table: table.name.clone(),
                expression: context.to_owned(),
            }),
            None => Ok(None),
        }),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => evaluate_scalar_expr(table, row, expr, context),
        _ => Err(ConstraintError::UnsupportedCheckExpression {
            table: table.name.clone(),
            expression: context.to_owned(),
        }),
    }
}

fn lookup_row_value(
    table: &TableCatalog,
    row: &BTreeMap<&str, &KernelValue>,
    ident: &Ident,
) -> Result<Option<ComparableValue>, ConstraintError> {
    row.get(ident.value.as_str())
        .copied()
        .ok_or_else(|| ConstraintError::UnknownColumn {
            table: table.name.clone(),
            column: ident.value.clone(),
        })
        .and_then(kernel_to_comparable)
}

fn kernel_to_comparable(value: &KernelValue) -> Result<Option<ComparableValue>, ConstraintError> {
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
            .map_err(|_| ConstraintError::UnsupportedCheckExpression {
                table: String::from("<runtime>"),
                expression: value.clone(),
            }),
        KernelValue::Float(value) => Ok(Some(ComparableValue::Float(*value))),
        KernelValue::Text(value) | KernelValue::Date(value) | KernelValue::Timestamp(value) => {
            Ok(Some(ComparableValue::Text(value.clone())))
        }
        KernelValue::Bytes(_) | KernelValue::Json(_) | KernelValue::Array(_) => {
            Err(ConstraintError::UnsupportedCheckExpression {
                table: String::from("<runtime>"),
                expression: String::from("non-scalar check value"),
            })
        }
    }
}

fn scalar_from_sql_value(
    value: &SqlValue,
    table: &TableCatalog,
    context: &str,
) -> Result<Option<ComparableValue>, ConstraintError> {
    match value {
        SqlValue::Null => Ok(None),
        SqlValue::Boolean(value) => Ok(Some(ComparableValue::Boolean(*value))),
        SqlValue::Number(value, _) => {
            if value.contains(['.', 'e', 'E']) {
                value
                    .parse::<f64>()
                    .map(ComparableValue::Float)
                    .map(Some)
                    .map_err(|_| ConstraintError::UnsupportedCheckExpression {
                        table: table.name.clone(),
                        expression: context.to_owned(),
                    })
            } else {
                value
                    .parse::<i128>()
                    .map(ComparableValue::Integer)
                    .map(Some)
                    .map_err(|_| ConstraintError::UnsupportedCheckExpression {
                        table: table.name.clone(),
                        expression: context.to_owned(),
                    })
            }
        }
        SqlValue::SingleQuotedString(value)
        | SqlValue::DoubleQuotedString(value)
        | SqlValue::EscapedStringLiteral(value)
        | SqlValue::UnicodeStringLiteral(value)
        | SqlValue::NationalStringLiteral(value)
        | SqlValue::TripleSingleQuotedString(value)
        | SqlValue::TripleDoubleQuotedString(value)
        | SqlValue::SingleQuotedRawStringLiteral(value)
        | SqlValue::DoubleQuotedRawStringLiteral(value)
        | SqlValue::TripleSingleQuotedRawStringLiteral(value)
        | SqlValue::TripleDoubleQuotedRawStringLiteral(value) => {
            Ok(Some(ComparableValue::Text(value.clone())))
        }
        _ => Err(ConstraintError::UnsupportedCheckExpression {
            table: table.name.clone(),
            expression: context.to_owned(),
        }),
    }
}

fn and3(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (Some(true), Some(true)) => Some(true),
        _ => None,
    }
}

fn or3(left: Option<bool>, right: Option<bool>) -> Option<bool> {
    match (left, right) {
        (Some(true), _) | (_, Some(true)) => Some(true),
        (Some(false), Some(false)) => Some(false),
        _ => None,
    }
}

#[derive(Debug, Clone)]
enum ComparableValue {
    Boolean(bool),
    Integer(i128),
    Float(f64),
    Text(String),
}
