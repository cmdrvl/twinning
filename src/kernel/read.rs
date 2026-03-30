use std::collections::BTreeMap;

use crate::{
    backend::{Backend, BackendError},
    catalog::{Catalog, ColumnCatalog, TableCatalog},
    ir::{
        AggregateKind, PredicateComparison, PredicateExpr, PredicateOperator, ReadOp, ReadShape,
        ScalarValue,
    },
    kernel::{
        coerce::{ClientInput, CoercionError, coerce_input},
        predicate::{PredicateError, predicate_matches},
        storage::{CommittedRow, TableStorage, TableStorageError},
        value::{KernelValue, ValueType, ValueTypeError},
    },
    result::{KernelResult, ReadResult, RefusalResult, ResultRow},
};

pub fn execute_read<B: Backend>(catalog: &Catalog, backend: &B, read: &ReadOp) -> KernelResult {
    match execute_read_inner(catalog, backend, read) {
        Ok(result) => KernelResult::Read(result),
        Err(refusal) => KernelResult::Refusal(refusal),
    }
}

fn execute_read_inner<B: Backend>(
    catalog: &Catalog,
    backend: &B,
    read: &ReadOp,
) -> Result<ReadResult, RefusalResult> {
    let table = catalog.table(&read.table).ok_or_else(|| {
        refusal(
            "unknown_table",
            format!("table `{}` is not present in the catalog", read.table),
            "42P01",
            [(String::from("table"), read.table.clone())],
        )
    })?;
    let committed = backend.base_table(&read.table).ok_or_else(|| {
        refusal_from_backend_error(BackendError::UnknownTable {
            table: read.table.clone(),
        })
    })?;

    let projected_rows = match read.shape {
        ReadShape::PointLookup => execute_point_lookup(table, committed, read)?,
        ReadShape::FilteredScan => execute_filtered_scan(table, committed, read)?,
        ReadShape::AggregateScan => execute_aggregate_scan(table, committed, read)?,
    };

    Ok(ReadResult {
        columns: result_columns(read),
        rows: apply_limit(projected_rows, read.limit),
    })
}

fn execute_point_lookup(
    table: &TableCatalog,
    committed: &TableStorage,
    read: &ReadOp,
) -> Result<Vec<ResultRow>, RefusalResult> {
    let lookup_values = point_lookup_values(table, read)?;
    let row = committed
        .lookup_primary_key(&lookup_values)
        .map_err(refusal_from_storage_error)?;

    row.map(|row| project_row(table.columns.as_slice(), &read.projection, row))
        .transpose()
        .map(|row| row.into_iter().collect())
}

fn execute_filtered_scan(
    table: &TableCatalog,
    committed: &TableStorage,
    read: &ReadOp,
) -> Result<Vec<ResultRow>, RefusalResult> {
    if read.predicate.is_none() {
        return Err(refusal(
            "missing_predicate",
            String::from("filtered scans require a declared predicate"),
            "0A000",
            [(String::from("table"), read.table.clone())],
        ));
    }

    matching_rows(table, committed, read.predicate.as_ref())?
        .into_iter()
        .map(|row| project_row(table.columns.as_slice(), &read.projection, row))
        .collect()
}

fn execute_aggregate_scan(
    table: &TableCatalog,
    committed: &TableStorage,
    read: &ReadOp,
) -> Result<Vec<ResultRow>, RefusalResult> {
    if read.aggregate.kind != AggregateKind::Count {
        return Err(refusal(
            "unsupported_read_shape",
            String::from("only COUNT aggregates are supported in this lane"),
            "0A000",
            [
                (String::from("table"), read.table.clone()),
                (
                    String::from("aggregate"),
                    aggregate_kind_token(read.aggregate.kind),
                ),
            ],
        ));
    }

    if !read.group_by.is_empty() && read.projection != read.group_by {
        return Err(refusal(
            "unsupported_read_shape",
            String::from("aggregate projection must match GROUP BY columns"),
            "0A000",
            [(String::from("table"), read.table.clone())],
        ));
    }

    let matching_rows = matching_rows(table, committed, read.predicate.as_ref())?;
    if read.group_by.is_empty() {
        let count = matching_rows
            .iter()
            .filter(|row| aggregate_row_counts(table, row, read.aggregate.column.as_ref()))
            .count();
        return Ok(vec![vec![count_to_scalar(count)?]]);
    }

    let mut grouped = BTreeMap::<String, (ResultRow, usize)>::new();
    for row in matching_rows {
        let group_values = project_row(table.columns.as_slice(), &read.group_by, row)?;
        let key = serde_json::to_string(&group_values)
            .expect("aggregate group key serialization should succeed");
        let bucket = grouped.entry(key).or_insert_with(|| (group_values, 0));
        if aggregate_row_counts(table, row, read.aggregate.column.as_ref()) {
            bucket.1 += 1;
        }
    }

    grouped
        .into_values()
        .map(|(mut group_values, count)| {
            group_values.push(count_to_scalar(count)?);
            Ok(group_values)
        })
        .collect()
}

fn point_lookup_values(
    table: &TableCatalog,
    read: &ReadOp,
) -> Result<Vec<KernelValue>, RefusalResult> {
    let primary_key = table.primary_key.as_ref().ok_or_else(|| {
        refusal(
            "missing_primary_key",
            format!(
                "table `{}` has no declared primary key lookup surface",
                table.name
            ),
            "0A000",
            [(String::from("table"), table.name.clone())],
        )
    })?;

    let comparisons = point_lookup_comparisons(read.predicate.as_ref())?;
    primary_key
        .columns
        .iter()
        .map(|column| {
            let comparison = comparisons.get(column).ok_or_else(|| {
                refusal(
                    "invalid_point_lookup",
                    format!("point lookup is missing equality predicate for `{column}`"),
                    "0A000",
                    [(String::from("column"), column.clone())],
                )
            })?;
            scalar_to_kernel_value(
                lookup_column(table.columns.as_slice(), column)?,
                &comparison.values[0],
            )
            .map_err(refusal_from_read_value_error)
        })
        .collect()
}

fn point_lookup_comparisons(
    predicate: Option<&PredicateExpr>,
) -> Result<BTreeMap<String, PredicateComparison>, RefusalResult> {
    let Some(predicate) = predicate else {
        return Err(refusal(
            "missing_predicate",
            String::from("point lookups require a declared predicate"),
            "0A000",
            [(String::from("shape"), String::from("point_lookup"))],
        ));
    };

    let comparisons = match predicate {
        PredicateExpr::Comparison(comparison) => vec![comparison.clone()],
        PredicateExpr::Conjunction(comparisons) => comparisons.clone(),
        PredicateExpr::Disjunction(_) => {
            return Err(refusal(
                "invalid_point_lookup",
                String::from("point lookups cannot use disjunction predicates"),
                "0A000",
                [(String::from("shape"), String::from("point_lookup"))],
            ));
        }
    };

    comparisons
        .into_iter()
        .map(|comparison| {
            if comparison.operator != PredicateOperator::Eq || comparison.values.len() != 1 {
                return Err(refusal(
                    "invalid_point_lookup",
                    format!(
                        "point lookup requires equality comparisons with one value, got `{}`",
                        comparison.column
                    ),
                    "0A000",
                    [(String::from("column"), comparison.column.clone())],
                ));
            }

            Ok((comparison.column.clone(), comparison))
        })
        .collect()
}

fn project_row(
    table_columns: &[ColumnCatalog],
    projection: &[String],
    row: &CommittedRow,
) -> Result<ResultRow, RefusalResult> {
    projection
        .iter()
        .map(|column| {
            let index = table_columns
                .iter()
                .position(|candidate| candidate.name == *column)
                .ok_or_else(|| {
                    refusal(
                        "unknown_projection_column",
                        format!("projection column `{column}` is not present in the catalog"),
                        "42703",
                        [(String::from("column"), column.clone())],
                    )
                })?;
            kernel_to_result_value(&row.values[index]).map_err(|message| {
                refusal(
                    "unsupported_read_value",
                    message,
                    "0A000",
                    [(String::from("column"), column.clone())],
                )
            })
        })
        .collect()
}

fn apply_limit(mut rows: Vec<ResultRow>, limit: Option<u64>) -> Vec<ResultRow> {
    if let Some(limit) = limit {
        rows.truncate(limit as usize);
    }

    rows
}

fn result_columns(read: &ReadOp) -> Vec<String> {
    if read.aggregate.kind == AggregateKind::None {
        return read.projection.clone();
    }

    let mut columns = read.projection.clone();
    columns.push(
        read.aggregate
            .alias
            .clone()
            .unwrap_or_else(|| String::from("count")),
    );
    columns
}

fn lookup_column<'a>(
    table_columns: &'a [ColumnCatalog],
    column: &str,
) -> Result<&'a ColumnCatalog, RefusalResult> {
    table_columns
        .iter()
        .find(|candidate| candidate.name == column)
        .ok_or_else(|| {
            refusal(
                "unknown_column",
                format!("column `{column}` is not present in the catalog"),
                "42703",
                [(String::from("column"), column.to_owned())],
            )
        })
}

fn scalar_to_kernel_value(
    column: &ColumnCatalog,
    value: &ScalarValue,
) -> Result<KernelValue, ReadValueError> {
    let declared_type =
        ValueType::from_normalized_catalog_type(&column.normalized_type).map_err(|source| {
            ReadValueError::UnsupportedColumnType {
                column: column.name.clone(),
                normalized_type: column.normalized_type.clone(),
                source,
            }
        })?;

    match value {
        ScalarValue::Null => Ok(KernelValue::Null),
        ScalarValue::Boolean(value) => match declared_type {
            ValueType::Boolean => Ok(KernelValue::Boolean(*value)),
            _ => coerce_input(&ClientInput::Text(value.to_string()), declared_type).map_err(
                |source| ReadValueError::Coercion {
                    column: column.name.clone(),
                    source,
                },
            ),
        },
        ScalarValue::Integer(value) => match declared_type {
            ValueType::Bigint => Ok(KernelValue::Bigint(*value)),
            ValueType::Integer => i32::try_from(*value)
                .map(KernelValue::Integer)
                .map_err(|_| ReadValueError::Coercion {
                    column: column.name.clone(),
                    source: CoercionError::InvalidSyntax {
                        declared_type,
                        input: value.to_string(),
                    },
                }),
            ValueType::Smallint => i16::try_from(*value)
                .map(KernelValue::Smallint)
                .map_err(|_| ReadValueError::Coercion {
                    column: column.name.clone(),
                    source: CoercionError::InvalidSyntax {
                        declared_type,
                        input: value.to_string(),
                    },
                }),
            ValueType::Numeric => Ok(KernelValue::Numeric(value.to_string())),
            ValueType::Float => Ok(KernelValue::Float(*value as f64)),
            ValueType::Text => Ok(KernelValue::Text(value.to_string())),
            _ => coerce_input(&ClientInput::Text(value.to_string()), declared_type).map_err(
                |source| ReadValueError::Coercion {
                    column: column.name.clone(),
                    source,
                },
            ),
        },
        ScalarValue::Text(value) => coerce_input(&ClientInput::Text(value.clone()), declared_type)
            .map_err(|source| ReadValueError::Coercion {
                column: column.name.clone(),
                source,
            }),
    }
}

fn matching_rows<'a>(
    table: &TableCatalog,
    committed: &'a TableStorage,
    predicate: Option<&PredicateExpr>,
) -> Result<Vec<&'a CommittedRow>, RefusalResult> {
    committed
        .rows()
        .filter_map(|row| match predicate {
            Some(predicate) => match predicate_matches(table, &row.values, predicate) {
                Ok(true) => Some(Ok(row)),
                Ok(false) => None,
                Err(error) => Some(Err(refusal_from_predicate_error(
                    table.name.as_str(),
                    error,
                ))),
            },
            None => Some(Ok(row)),
        })
        .collect()
}

fn aggregate_row_counts(
    table: &TableCatalog,
    row: &CommittedRow,
    aggregate_column: Option<&String>,
) -> bool {
    let Some(column) = aggregate_column else {
        return true;
    };

    let index = table
        .columns
        .iter()
        .position(|candidate| candidate.name == *column)
        .expect("aggregate column should exist in catalog");
    !row.values[index].is_null()
}

fn count_to_scalar(count: usize) -> Result<ScalarValue, RefusalResult> {
    i64::try_from(count).map(ScalarValue::Integer).map_err(|_| {
        refusal(
            "aggregate_count_overflow",
            String::from("aggregate row count exceeded i64"),
            "XX000",
            [(String::from("count"), count.to_string())],
        )
    })
}

fn aggregate_kind_token(kind: AggregateKind) -> String {
    match kind {
        AggregateKind::None => String::from("none"),
        AggregateKind::Count => String::from("count"),
        AggregateKind::Sum => String::from("sum"),
        AggregateKind::Avg => String::from("avg"),
        AggregateKind::Min => String::from("min"),
        AggregateKind::Max => String::from("max"),
    }
}

fn kernel_to_result_value(value: &KernelValue) -> Result<ScalarValue, String> {
    match value {
        KernelValue::Null => Ok(ScalarValue::Null),
        KernelValue::Boolean(value) => Ok(ScalarValue::Boolean(*value)),
        KernelValue::Smallint(value) => Ok(ScalarValue::Integer(i64::from(*value))),
        KernelValue::Integer(value) => Ok(ScalarValue::Integer(i64::from(*value))),
        KernelValue::Bigint(value) => Ok(ScalarValue::Integer(*value)),
        KernelValue::Numeric(value)
        | KernelValue::Timestamp(value)
        | KernelValue::Date(value)
        | KernelValue::Text(value) => Ok(ScalarValue::Text(value.clone())),
        KernelValue::Float(value) => Ok(ScalarValue::Text(value.to_string())),
        KernelValue::Bytes(_) | KernelValue::Json(_) | KernelValue::Array(_) => Err(String::from(
            "read results currently support only null, boolean, integer, and text-like scalar values",
        )),
    }
}

fn refusal_from_storage_error(error: TableStorageError) -> RefusalResult {
    refusal(
        "storage_error",
        error.to_string(),
        "XX000",
        [(String::from("error"), error.to_string())],
    )
}

fn refusal_from_backend_error(error: BackendError) -> RefusalResult {
    match error {
        BackendError::UnknownTable { table } => refusal(
            "unknown_table",
            format!("backend has no table named `{table}`"),
            "42P01",
            [(String::from("table"), table)],
        ),
        BackendError::DuplicateBaseTable { table } => refusal(
            "backend_error",
            format!("backend already contains duplicate table `{table}`"),
            "XX000",
            [(String::from("table"), table)],
        ),
    }
}

fn refusal_from_predicate_error(table: &str, error: PredicateError) -> RefusalResult {
    match error {
        PredicateError::UnknownColumn { column, .. } => refusal(
            "unknown_column",
            format!("predicate references unknown column `{column}`"),
            "42703",
            [(String::from("column"), column)],
        ),
        PredicateError::UnsupportedValueKind { column, kind, .. } => refusal(
            "unsupported_predicate_value",
            format!("predicate on `{table}.{column}` does not support `{kind}` values"),
            "0A000",
            [
                (String::from("table"), table.to_owned()),
                (String::from("column"), column),
                (String::from("kind"), kind),
            ],
        ),
        other => refusal(
            "predicate_error",
            other.to_string(),
            "0A000",
            [(String::from("error"), other.to_string())],
        ),
    }
}

fn refusal_from_read_value_error(error: ReadValueError) -> RefusalResult {
    match error {
        ReadValueError::UnsupportedColumnType {
            column,
            normalized_type,
            source,
        } => refusal(
            "unsupported_column_type",
            source.to_string(),
            "0A000",
            [
                (String::from("column"), column),
                (String::from("normalized_type"), normalized_type),
            ],
        ),
        ReadValueError::Coercion { column, source } => refusal(
            "invalid_text_representation",
            source.to_string(),
            source.sqlstate(),
            [(String::from("column"), column)],
        ),
    }
}

fn refusal<const N: usize>(
    code: &str,
    message: String,
    sqlstate: &str,
    detail: [(String, String); N],
) -> RefusalResult {
    RefusalResult {
        code: code.to_owned(),
        message,
        sqlstate: sqlstate.to_owned(),
        detail: BTreeMap::from(detail),
    }
}

#[derive(Debug)]
enum ReadValueError {
    UnsupportedColumnType {
        column: String,
        normalized_type: String,
        source: ValueTypeError,
    },
    Coercion {
        column: String,
        source: CoercionError,
    },
}
