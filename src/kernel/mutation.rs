use std::collections::{BTreeMap, BTreeSet};

use crate::{
    backend::{Backend, BackendError},
    catalog::{Catalog, ColumnCatalog, TableCatalog},
    ir::{ConflictTarget, MutationKind, MutationOp, PredicateExpr, ScalarValue},
    kernel::{
        coerce::{ClientInput, CoercionError, coerce_input},
        constraints::{ConstraintError, enforce_insert_constraints},
        predicate::{PredicateError, predicate_matches},
        storage::{CommittedRow, TableStorage, TableStorageError},
        value::{KernelValue, ValueType, ValueTypeError},
    },
    result::{KernelResult, MutationResult, RefusalResult, ResultRow, ResultTag},
};

pub fn execute_mutation<B: Backend>(
    catalog: &Catalog,
    backend: &mut B,
    mutation: &MutationOp,
) -> KernelResult {
    match execute_mutation_inner(catalog, backend, mutation) {
        Ok(result) => KernelResult::Mutation(result),
        Err(refusal) => KernelResult::Refusal(refusal),
    }
}

pub fn execute_insert<B: Backend>(
    catalog: &Catalog,
    backend: &mut B,
    mutation: &MutationOp,
) -> KernelResult {
    execute_mutation(catalog, backend, mutation)
}

fn execute_mutation_inner<B: Backend>(
    catalog: &Catalog,
    backend: &mut B,
    mutation: &MutationOp,
) -> Result<MutationResult, RefusalResult> {
    match mutation.kind {
        MutationKind::Insert | MutationKind::Upsert => {
            execute_insert_inner(catalog, backend, mutation)
        }
        MutationKind::Update => execute_update_inner(catalog, backend, mutation),
        MutationKind::Delete => execute_delete_inner(catalog, backend, mutation),
    }
}

fn execute_insert_inner<B: Backend>(
    catalog: &Catalog,
    backend: &mut B,
    mutation: &MutationOp,
) -> Result<MutationResult, RefusalResult> {
    if !matches!(mutation.kind, MutationKind::Insert | MutationKind::Upsert) {
        return Err(refusal(
            "unsupported_mutation_kind",
            format!(
                "mutation kind `{}` is outside the INSERT/UPSERT execution lane",
                mutation_kind_token(mutation.kind)
            ),
            "0A000",
            [(String::from("table"), mutation.table.clone())],
        ));
    }

    let (table, mut visible_table) = load_mutation_table(catalog, backend, &mutation.table)?;

    let mut returning_rows = Vec::with_capacity(mutation.rows.len());
    for row in &mutation.rows {
        let full_row = build_insert_row(table.columns.as_slice(), &mutation.columns, row)
            .map_err(refusal_from_row_error)?;

        let conflict_row_id = match mutation.kind {
            MutationKind::Insert => None,
            MutationKind::Upsert => find_conflicting_row_id(
                table,
                &visible_table,
                mutation.conflict_target.as_ref(),
                &full_row,
            )?,
            MutationKind::Update | MutationKind::Delete => unreachable!("dispatched above"),
        };

        if let Some(conflict_row_id) = conflict_row_id {
            let conflicting_row = visible_table
                .rows()
                .find(|row| row.id == conflict_row_id)
                .ok_or_else(|| {
                    refusal(
                        "missing_conflict_row",
                        format!(
                            "conflict target resolved to missing row id `{conflict_row_id}` on `{}`",
                            table.name
                        ),
                        "0A000",
                        [(String::from("table"), table.name.clone())],
                    )
                })?;
            let merged_row = merge_upsert_row(
                table.columns.as_slice(),
                &conflicting_row.values,
                &full_row,
                &mutation.update_columns,
            )?;
            let validation_table =
                rebuild_table(table, &visible_table, Some(conflict_row_id), None)?;
            let candidate_table = rebuild_table(
                table,
                &visible_table,
                Some(conflict_row_id),
                Some(&merged_row),
            )?;
            enforce_insert_constraints(table, &validation_table, &merged_row, |table_name| {
                if table_name == table.name {
                    Some(&candidate_table)
                } else {
                    backend.visible_table(table_name)
                }
            })
            .map_err(refusal_from_constraint_error)?;
            validate_referencing_rows(catalog, backend, table, &candidate_table)?;
            visible_table = candidate_table;
            if !mutation.returning.is_empty() {
                returning_rows.push(extract_returning_row(
                    table.columns.as_slice(),
                    &mutation.returning,
                    &merged_row,
                )?);
            }
        } else {
            enforce_insert_constraints(table, &visible_table, &full_row, |table_name| {
                if table_name == table.name {
                    Some(&visible_table)
                } else {
                    backend.visible_table(table_name)
                }
            })
            .map_err(refusal_from_constraint_error)?;
            visible_table
                .insert_row(full_row.clone())
                .map_err(refusal_from_storage_error)?;
            validate_referencing_rows(catalog, backend, table, &visible_table)?;
            if !mutation.returning.is_empty() {
                returning_rows.push(extract_returning_row(
                    table.columns.as_slice(),
                    &mutation.returning,
                    &full_row,
                )?);
            }
        }
    }

    backend
        .write_overlay_table(visible_table)
        .map_err(refusal_from_backend_error)?;

    Ok(MutationResult {
        tag: ResultTag::from(mutation.kind),
        rows_affected: mutation.rows.len() as u64,
        returning_rows,
    })
}

fn execute_update_inner<B: Backend>(
    catalog: &Catalog,
    backend: &mut B,
    mutation: &MutationOp,
) -> Result<MutationResult, RefusalResult> {
    validate_update_shape(mutation)?;
    let (table, visible_table) = load_mutation_table(catalog, backend, &mutation.table)?;
    let assignments = build_update_assignments(
        table.columns.as_slice(),
        &mutation.columns,
        &mutation.rows[0],
    )?;
    let matched_rows = matching_rows(table, &visible_table, mutation.predicate.as_ref())?;

    if matched_rows.is_empty() {
        return Ok(MutationResult {
            tag: ResultTag::Update,
            rows_affected: 0,
            returning_rows: Vec::new(),
        });
    }

    let matched_ids = matched_rows
        .iter()
        .map(|row| row.id)
        .collect::<BTreeSet<_>>();
    let mut updated_rows = Vec::with_capacity(matched_rows.len());
    let final_rows = visible_table
        .rows()
        .map(|row| {
            if matched_ids.contains(&row.id) {
                let updated = apply_assignments(&row.values, &assignments);
                updated_rows.push(updated.clone());
                updated
            } else {
                row.values.clone()
            }
        })
        .collect::<Vec<_>>();

    let candidate_table = build_candidate_table(catalog, backend, table, &final_rows)?;
    backend
        .write_overlay_table(candidate_table)
        .map_err(refusal_from_backend_error)?;

    Ok(MutationResult {
        tag: ResultTag::Update,
        rows_affected: matched_rows.len() as u64,
        returning_rows: extract_returning_rows(
            table.columns.as_slice(),
            &mutation.returning,
            &updated_rows,
        )?,
    })
}

fn execute_delete_inner<B: Backend>(
    catalog: &Catalog,
    backend: &mut B,
    mutation: &MutationOp,
) -> Result<MutationResult, RefusalResult> {
    validate_delete_shape(mutation)?;
    let (table, visible_table) = load_mutation_table(catalog, backend, &mutation.table)?;
    let matched_rows = matching_rows(table, &visible_table, mutation.predicate.as_ref())?;

    if matched_rows.is_empty() {
        return Ok(MutationResult {
            tag: ResultTag::Delete,
            rows_affected: 0,
            returning_rows: Vec::new(),
        });
    }

    let matched_ids = matched_rows
        .iter()
        .map(|row| row.id)
        .collect::<BTreeSet<_>>();
    let deleted_rows = matched_rows
        .iter()
        .map(|row| row.values.clone())
        .collect::<Vec<_>>();
    let final_rows = visible_table
        .rows()
        .filter(|row| !matched_ids.contains(&row.id))
        .map(|row| row.values.clone())
        .collect::<Vec<_>>();

    let candidate_table = build_candidate_table(catalog, backend, table, &final_rows)?;
    backend
        .write_overlay_table(candidate_table)
        .map_err(refusal_from_backend_error)?;

    Ok(MutationResult {
        tag: ResultTag::Delete,
        rows_affected: matched_rows.len() as u64,
        returning_rows: extract_returning_rows(
            table.columns.as_slice(),
            &mutation.returning,
            &deleted_rows,
        )?,
    })
}

fn validate_update_shape(mutation: &MutationOp) -> Result<(), RefusalResult> {
    if mutation.conflict_target.is_some() {
        return Err(unsupported_mutation_shape(
            mutation,
            "UPDATE does not accept a conflict target",
        ));
    }
    if mutation.predicate.is_none() {
        return Err(missing_mutation_predicate(mutation));
    }
    if mutation.rows.len() != 1 {
        return Err(unsupported_mutation_shape(
            mutation,
            "UPDATE supports exactly one assignment row in this lane",
        ));
    }
    if mutation.columns.is_empty() {
        return Err(unsupported_mutation_shape(
            mutation,
            "UPDATE requires at least one assignment column",
        ));
    }

    Ok(())
}

fn validate_delete_shape(mutation: &MutationOp) -> Result<(), RefusalResult> {
    if mutation.conflict_target.is_some() {
        return Err(unsupported_mutation_shape(
            mutation,
            "DELETE does not accept a conflict target",
        ));
    }
    if mutation.predicate.is_none() {
        return Err(missing_mutation_predicate(mutation));
    }
    if !mutation.columns.is_empty() || !mutation.rows.is_empty() {
        return Err(unsupported_mutation_shape(
            mutation,
            "DELETE does not carry assignment columns or row payloads",
        ));
    }

    Ok(())
}

fn missing_mutation_predicate(mutation: &MutationOp) -> RefusalResult {
    refusal(
        "missing_predicate",
        format!(
            "{} mutations require a declared predicate",
            mutation_kind_token(mutation.kind).to_ascii_uppercase()
        ),
        "0A000",
        [(String::from("table"), mutation.table.clone())],
    )
}

fn unsupported_mutation_shape(mutation: &MutationOp, message: &str) -> RefusalResult {
    refusal(
        "unsupported_mutation_shape",
        message.to_owned(),
        "0A000",
        [
            (String::from("table"), mutation.table.clone()),
            (
                String::from("kind"),
                mutation_kind_token(mutation.kind).to_owned(),
            ),
        ],
    )
}

fn mutation_kind_token(kind: MutationKind) -> &'static str {
    match kind {
        MutationKind::Insert => "insert",
        MutationKind::Upsert => "upsert",
        MutationKind::Update => "update",
        MutationKind::Delete => "delete",
    }
}

fn load_mutation_table<'a, B: Backend>(
    catalog: &'a Catalog,
    backend: &B,
    table_name: &str,
) -> Result<(&'a TableCatalog, TableStorage), RefusalResult> {
    let table = catalog.table(table_name).ok_or_else(|| {
        refusal(
            "unknown_table",
            format!("table `{table_name}` is not present in the catalog"),
            "42P01",
            [(String::from("table"), table_name.to_owned())],
        )
    })?;
    let visible_table = backend.visible_table(table_name).cloned().ok_or_else(|| {
        refusal(
            "unknown_table",
            format!("backend has no visible table `{table_name}`"),
            "42P01",
            [(String::from("table"), table_name.to_owned())],
        )
    })?;

    Ok((table, visible_table))
}

fn build_update_assignments(
    table_columns: &[ColumnCatalog],
    mutation_columns: &[String],
    row: &[ScalarValue],
) -> Result<BTreeMap<usize, KernelValue>, RefusalResult> {
    if mutation_columns.len() != row.len() {
        return Err(refusal_from_row_error(RowBuildError::Arity {
            expected: mutation_columns.len(),
            actual: row.len(),
        }));
    }

    let mut assignments = BTreeMap::new();
    for (column, value) in mutation_columns.iter().zip(row.iter()) {
        let index = table_columns
            .iter()
            .position(|candidate| candidate.name == *column)
            .ok_or_else(|| {
                refusal(
                    "unknown_column",
                    format!("column `{column}` is not present in the catalog"),
                    "42703",
                    [(String::from("column"), column.clone())],
                )
            })?;
        if assignments.contains_key(&index) {
            return Err(refusal(
                "duplicate_column",
                format!("column `{column}` appears more than once in the mutation payload"),
                "42701",
                [(String::from("column"), column.clone())],
            ));
        }

        let declared_type =
            ValueType::from_normalized_catalog_type(&table_columns[index].normalized_type)
                .map_err(|source| {
                    refusal_from_row_error(RowBuildError::UnsupportedColumnType {
                        column: column.clone(),
                        normalized_type: table_columns[index].normalized_type.clone(),
                        source,
                    })
                })?;
        let kernel_value = scalar_to_kernel_value(value, declared_type).map_err(|source| {
            refusal_from_row_error(RowBuildError::Coercion {
                column: column.clone(),
                source,
            })
        })?;
        assignments.insert(index, kernel_value);
    }

    Ok(assignments)
}

fn apply_assignments(
    original: &[KernelValue],
    assignments: &BTreeMap<usize, KernelValue>,
) -> Vec<KernelValue> {
    original
        .iter()
        .enumerate()
        .map(|(index, value)| {
            assignments
                .get(&index)
                .cloned()
                .unwrap_or_else(|| value.clone())
        })
        .collect()
}

fn matching_rows<'a>(
    table: &TableCatalog,
    visible_table: &'a TableStorage,
    predicate: Option<&PredicateExpr>,
) -> Result<Vec<&'a CommittedRow>, RefusalResult> {
    let predicate = predicate.expect("validated mutation predicate should be present");
    visible_table
        .rows()
        .filter_map(
            |row| match predicate_matches(table, &row.values, predicate) {
                Ok(true) => Some(Ok(row)),
                Ok(false) => None,
                Err(error) => Some(Err(refusal_from_predicate_error(
                    table.name.as_str(),
                    error,
                ))),
            },
        )
        .collect()
}

fn build_candidate_table<B: Backend>(
    catalog: &Catalog,
    backend: &B,
    table: &TableCatalog,
    final_rows: &[Vec<KernelValue>],
) -> Result<TableStorage, RefusalResult> {
    let mut candidate = TableStorage::new(table).map_err(refusal_from_storage_error)?;

    for row in final_rows {
        enforce_insert_constraints(table, &candidate, row, |table_name| {
            if table_name == table.name {
                Some(&candidate)
            } else {
                backend.visible_table(table_name)
            }
        })
        .map_err(refusal_from_constraint_error)?;
        candidate
            .insert_row(row.clone())
            .map_err(refusal_from_storage_error)?;
    }

    validate_referencing_rows(catalog, backend, table, &candidate)?;
    Ok(candidate)
}

fn validate_referencing_rows<B: Backend>(
    catalog: &Catalog,
    backend: &B,
    target_table: &TableCatalog,
    candidate_table: &TableStorage,
) -> Result<(), RefusalResult> {
    for table in &catalog.tables {
        let visible_table = if table.name == target_table.name {
            candidate_table
        } else {
            backend.visible_table(&table.name).ok_or_else(|| {
                refusal(
                    "unknown_table",
                    format!("backend has no visible table `{}`", table.name),
                    "42P01",
                    [(String::from("table"), table.name.clone())],
                )
            })?
        };

        for foreign_key in table
            .foreign_keys
            .iter()
            .filter(|foreign_key| foreign_key.foreign_table == target_table.name)
        {
            for row in visible_table.rows() {
                let lookup_values =
                    lookup_fk_values(table.columns.as_slice(), &row.values, &foreign_key.columns)?;
                if lookup_values.iter().any(KernelValue::is_null) {
                    continue;
                }

                if !candidate_has_lookup_surface(
                    candidate_table,
                    &foreign_key.referred_columns,
                    &lookup_values,
                )? {
                    return Err(refusal_from_constraint_error(
                        ConstraintError::ForeignKeyViolation {
                            table: table.name.clone(),
                            columns: foreign_key.columns.clone(),
                            foreign_table: foreign_key.foreign_table.clone(),
                            referred_columns: foreign_key.referred_columns.clone(),
                            name: foreign_key.name.clone(),
                        },
                    ));
                }
            }
        }
    }

    Ok(())
}

fn candidate_has_lookup_surface(
    candidate_table: &TableStorage,
    referred_columns: &[String],
    lookup_values: &[KernelValue],
) -> Result<bool, RefusalResult> {
    if candidate_table
        .primary_key_surface()
        .is_some_and(|surface| surface.columns == referred_columns)
    {
        return candidate_table
            .lookup_primary_key(lookup_values)
            .map(|row| row.is_some())
            .map_err(refusal_from_storage_error);
    }

    if candidate_table
        .unique_surfaces()
        .iter()
        .any(|surface| surface.columns == referred_columns)
    {
        let columns = referred_columns
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        return candidate_table
            .lookup_unique(&columns, lookup_values)
            .map(|row| row.is_some())
            .map_err(refusal_from_storage_error);
    }

    Ok(false)
}

fn lookup_fk_values(
    table_columns: &[ColumnCatalog],
    row: &[KernelValue],
    lookup_columns: &[String],
) -> Result<Vec<KernelValue>, RefusalResult> {
    lookup_columns
        .iter()
        .map(|lookup_column| {
            let index = table_columns
                .iter()
                .position(|column| column.name == *lookup_column)
                .ok_or_else(|| {
                    refusal(
                        "unknown_column",
                        format!("column `{lookup_column}` is not present in the table"),
                        "42703",
                        [(String::from("column"), lookup_column.clone())],
                    )
                })?;
            Ok(row[index].clone())
        })
        .collect()
}

fn find_conflicting_row_id(
    table: &TableCatalog,
    visible_table: &TableStorage,
    conflict_target: Option<&ConflictTarget>,
    row: &[KernelValue],
) -> Result<Option<u64>, RefusalResult> {
    let conflict_target = conflict_target.ok_or_else(|| {
        refusal(
            "missing_conflict_target",
            String::from("UPSERT requires a declared conflict target"),
            "0A000",
            [(String::from("table"), table.name.clone())],
        )
    })?;
    let target_columns = conflict_target_columns(table, conflict_target)?;
    let values = lookup_row_values(table.columns.as_slice(), row, target_columns.as_slice())?;
    if values.iter().any(KernelValue::is_null) {
        return Ok(None);
    }

    match conflict_target {
        ConflictTarget::PrimaryKey => visible_table
            .lookup_primary_key(&values)
            .map(|row| row.map(|row| row.id))
            .map_err(refusal_from_storage_error),
        ConflictTarget::Columns(columns) => {
            lookup_unique_conflict_row_id(visible_table, columns, &values)
        }
        ConflictTarget::NamedConstraint(name) => {
            let columns = table
                .unique_constraints
                .iter()
                .find(|constraint| constraint.name.as_deref() == Some(name.as_str()))
                .map(|constraint| constraint.columns.clone())
                .ok_or_else(|| {
                    refusal(
                        "unknown_conflict_target",
                        format!(
                            "constraint `{name}` is not a declared unique surface on `{}`",
                            table.name
                        ),
                        "0A000",
                        [
                            (String::from("table"), table.name.clone()),
                            (String::from("target"), name.clone()),
                        ],
                    )
                })?;
            lookup_unique_conflict_row_id(visible_table, columns.as_slice(), &values)
        }
    }
}

fn lookup_unique_conflict_row_id(
    visible_table: &TableStorage,
    columns: &[String],
    values: &[KernelValue],
) -> Result<Option<u64>, RefusalResult> {
    let column_refs = columns.iter().map(String::as_str).collect::<Vec<_>>();
    visible_table
        .lookup_unique(&column_refs, values)
        .map(|row| row.map(|row| row.id))
        .map_err(|error| match error {
            TableStorageError::UnknownUniqueSurface { table, columns } => refusal(
                "unknown_conflict_target",
                format!(
                    "columns {:?} are not a declared unique surface on `{table}`",
                    columns
                ),
                "0A000",
                [
                    (String::from("table"), table),
                    (String::from("target"), columns.join(",")),
                ],
            ),
            other => refusal_from_storage_error(other),
        })
}

fn conflict_target_columns(
    table: &TableCatalog,
    conflict_target: &ConflictTarget,
) -> Result<Vec<String>, RefusalResult> {
    match conflict_target {
        ConflictTarget::PrimaryKey => table
            .primary_key
            .as_ref()
            .map(|key| key.columns.clone())
            .ok_or_else(|| {
                refusal(
                    "unknown_conflict_target",
                    format!(
                        "table `{}` has no declared primary key conflict surface",
                        table.name
                    ),
                    "0A000",
                    [(String::from("table"), table.name.clone())],
                )
            }),
        ConflictTarget::Columns(columns) => Ok(columns.clone()),
        ConflictTarget::NamedConstraint(name) => table
            .unique_constraints
            .iter()
            .find(|constraint| constraint.name.as_deref() == Some(name.as_str()))
            .map(|constraint| constraint.columns.clone())
            .ok_or_else(|| {
                refusal(
                    "unknown_conflict_target",
                    format!(
                        "constraint `{name}` is not a declared unique surface on `{}`",
                        table.name
                    ),
                    "0A000",
                    [
                        (String::from("table"), table.name.clone()),
                        (String::from("target"), name.clone()),
                    ],
                )
            }),
    }
}

fn lookup_row_values(
    table_columns: &[ColumnCatalog],
    row: &[KernelValue],
    lookup_columns: &[String],
) -> Result<Vec<KernelValue>, RefusalResult> {
    lookup_columns
        .iter()
        .map(|lookup_column| {
            let index = table_columns
                .iter()
                .position(|column| column.name == *lookup_column)
                .ok_or_else(|| {
                    refusal(
                        "unknown_conflict_target",
                        format!("column `{lookup_column}` is not present in the table"),
                        "0A000",
                        [(String::from("column"), lookup_column.clone())],
                    )
                })?;
            Ok(row[index].clone())
        })
        .collect()
}

fn rebuild_table(
    table: &TableCatalog,
    visible_table: &TableStorage,
    replaced_row_id: Option<u64>,
    replacement: Option<&[KernelValue]>,
) -> Result<TableStorage, RefusalResult> {
    let mut rebuilt = TableStorage::new(table).map_err(refusal_from_storage_error)?;

    for committed_row in visible_table.rows() {
        if Some(committed_row.id) == replaced_row_id {
            if let Some(replacement) = replacement {
                rebuilt
                    .insert_row(replacement.to_vec())
                    .map_err(refusal_from_storage_error)?;
            }
        } else {
            rebuilt
                .insert_row(committed_row.values.clone())
                .map_err(refusal_from_storage_error)?;
        }
    }

    Ok(rebuilt)
}

fn build_insert_row(
    table_columns: &[ColumnCatalog],
    mutation_columns: &[String],
    row: &[ScalarValue],
) -> Result<Vec<KernelValue>, RowBuildError> {
    if mutation_columns.len() != row.len() {
        return Err(RowBuildError::Arity {
            expected: mutation_columns.len(),
            actual: row.len(),
        });
    }

    let provided = mutation_columns
        .iter()
        .cloned()
        .zip(row.iter())
        .collect::<BTreeMap<_, _>>();

    table_columns
        .iter()
        .map(|column| match provided.get(&column.name) {
            Some(value) => {
                let declared_type = ValueType::from_normalized_catalog_type(
                    &column.normalized_type,
                )
                .map_err(|source| RowBuildError::UnsupportedColumnType {
                    column: column.name.clone(),
                    normalized_type: column.normalized_type.clone(),
                    source,
                })?;
                scalar_to_kernel_value(value, declared_type).map_err(|source| {
                    RowBuildError::Coercion {
                        column: column.name.clone(),
                        source,
                    }
                })
            }
            None => {
                if column.default_sql.is_some() {
                    Err(RowBuildError::DefaultExpression {
                        column: column.name.clone(),
                    })
                } else {
                    Ok(KernelValue::Null)
                }
            }
        })
        .collect()
}

fn merge_upsert_row(
    table_columns: &[ColumnCatalog],
    existing_row: &[KernelValue],
    inserted_row: &[KernelValue],
    update_columns: &[String],
) -> Result<Vec<KernelValue>, RefusalResult> {
    let mut merged = existing_row.to_vec();

    for column_name in update_columns {
        let index = table_columns
            .iter()
            .position(|column| column.name == *column_name)
            .ok_or_else(|| {
                refusal(
                    "unknown_column",
                    format!("column `{column_name}` is not present in the catalog"),
                    "42703",
                    [(String::from("column"), column_name.clone())],
                )
            })?;
        merged[index] = inserted_row[index].clone();
    }

    Ok(merged)
}

fn scalar_to_kernel_value(
    value: &ScalarValue,
    declared_type: ValueType,
) -> Result<KernelValue, CoercionError> {
    match value {
        ScalarValue::Null => Ok(KernelValue::Null),
        ScalarValue::Boolean(value) => match declared_type {
            ValueType::Boolean => Ok(KernelValue::Boolean(*value)),
            _ => coerce_input(&ClientInput::Text(value.to_string()), declared_type),
        },
        ScalarValue::Integer(value) => match declared_type {
            ValueType::Bigint => Ok(KernelValue::Bigint(*value)),
            ValueType::Integer => i32::try_from(*value)
                .map(KernelValue::Integer)
                .map_err(|_| CoercionError::InvalidSyntax {
                    declared_type,
                    input: value.to_string(),
                }),
            ValueType::Smallint => i16::try_from(*value)
                .map(KernelValue::Smallint)
                .map_err(|_| CoercionError::InvalidSyntax {
                    declared_type,
                    input: value.to_string(),
                }),
            ValueType::Numeric => Ok(KernelValue::Numeric(value.to_string())),
            ValueType::Float => Ok(KernelValue::Float(*value as f64)),
            ValueType::Text => Ok(KernelValue::Text(value.to_string())),
            _ => coerce_input(&ClientInput::Text(value.to_string()), declared_type),
        },
        ScalarValue::Text(value) => coerce_input(&ClientInput::Text(value.clone()), declared_type),
    }
}

fn extract_returning_rows(
    table_columns: &[ColumnCatalog],
    returning_columns: &[String],
    rows: &[Vec<KernelValue>],
) -> Result<Vec<ResultRow>, RefusalResult> {
    rows.iter()
        .map(|row| extract_returning_row(table_columns, returning_columns, row))
        .collect()
}

fn extract_returning_row(
    table_columns: &[ColumnCatalog],
    returning_columns: &[String],
    row: &[KernelValue],
) -> Result<ResultRow, RefusalResult> {
    returning_columns
        .iter()
        .map(|column| {
            let index = table_columns
                .iter()
                .position(|candidate| candidate.name == *column)
                .ok_or_else(|| {
                    refusal(
                        "unknown_returning_column",
                        format!("RETURNING column `{column}` is not present in the catalog"),
                        "42703",
                        [(String::from("column"), column.clone())],
                    )
                })?;
            kernel_to_result_value(&row[index]).map_err(|message| {
                refusal(
                    "unsupported_returning_value",
                    message,
                    "0A000",
                    [(String::from("column"), column.clone())],
                )
            })
        })
        .collect()
}

fn kernel_to_result_value(value: &KernelValue) -> Result<ScalarValue, String> {
    match value {
        KernelValue::Null => Ok(ScalarValue::Null),
        KernelValue::Boolean(value) => Ok(ScalarValue::Boolean(*value)),
        KernelValue::Smallint(value) => Ok(ScalarValue::Integer(i64::from(*value))),
        KernelValue::Integer(value) => Ok(ScalarValue::Integer(i64::from(*value))),
        KernelValue::Bigint(value) => Ok(ScalarValue::Integer(*value)),
        KernelValue::Numeric(value) => Ok(ScalarValue::Text(value.clone())),
        KernelValue::Float(value) => Ok(ScalarValue::Text(value.to_string())),
        KernelValue::Timestamp(value) => Ok(ScalarValue::Text(value.clone())),
        KernelValue::Date(value) => Ok(ScalarValue::Text(value.clone())),
        KernelValue::Text(value) => Ok(ScalarValue::Text(value.clone())),
        KernelValue::Bytes(_) | KernelValue::Json(_) | KernelValue::Array(_) => Err(String::from(
            "RETURNING currently supports only null, boolean, integer, and text-like scalar values",
        )),
    }
}

fn refusal_from_constraint_error(error: ConstraintError) -> RefusalResult {
    match error {
        ConstraintError::NotNullViolation { table, column } => refusal(
            "not_null_violation",
            format!("column `{column}` on `{table}` violates NOT NULL"),
            "23502",
            [
                (String::from("table"), table),
                (String::from("column"), column),
            ],
        ),
        ConstraintError::UniqueViolation {
            table,
            columns,
            name,
        } => refusal(
            "unique_violation",
            format!(
                "unique surface {:?} on `{table}` is already present",
                columns
            ),
            "23505",
            [
                (String::from("table"), table),
                (String::from("columns"), columns.join(",")),
                (
                    String::from("constraint"),
                    name.unwrap_or_else(|| String::from("<anonymous>")),
                ),
            ],
        ),
        ConstraintError::ForeignKeyViolation {
            table,
            columns,
            foreign_table,
            referred_columns,
            name,
        } => refusal(
            "foreign_key_violation",
            format!(
                "foreign key {:?} on `{table}` references missing row in `{foreign_table}`",
                columns
            ),
            "23503",
            [
                (String::from("table"), table),
                (String::from("columns"), columns.join(",")),
                (String::from("foreign_table"), foreign_table),
                (String::from("referred_columns"), referred_columns.join(",")),
                (
                    String::from("constraint"),
                    name.unwrap_or_else(|| String::from("<anonymous>")),
                ),
            ],
        ),
        ConstraintError::CheckViolation {
            table,
            expression,
            name,
        } => refusal(
            "check_violation",
            format!("CHECK constraint failed on `{table}`"),
            "23514",
            [
                (String::from("table"), table),
                (String::from("expression"), expression),
                (
                    String::from("constraint"),
                    name.unwrap_or_else(|| String::from("<anonymous>")),
                ),
            ],
        ),
        ConstraintError::RowArity {
            table,
            expected,
            actual,
        } => refusal(
            "row_arity",
            format!("row arity {actual} does not match expected {expected}"),
            "XX000",
            [
                (String::from("table"), table),
                (String::from("expected"), expected.to_string()),
                (String::from("actual"), actual.to_string()),
            ],
        ),
        ConstraintError::UnknownColumn { table, column } => refusal(
            "unknown_column",
            format!("column `{column}` is not present on `{table}`"),
            "XX000",
            [
                (String::from("table"), table),
                (String::from("column"), column),
            ],
        ),
        ConstraintError::UnknownRelatedTable {
            table,
            foreign_table,
        } => refusal(
            "unknown_related_table",
            format!("related table `{foreign_table}` is missing for `{table}`"),
            "XX000",
            [
                (String::from("table"), table),
                (String::from("foreign_table"), foreign_table),
            ],
        ),
        ConstraintError::UnsupportedCheckExpression { table, expression } => refusal(
            "unsupported_check_expression",
            format!("CHECK expression on `{table}` is outside the declared subset"),
            "XX000",
            [
                (String::from("table"), table),
                (String::from("expression"), expression),
            ],
        ),
    }
}

fn refusal_from_storage_error(error: TableStorageError) -> RefusalResult {
    match error {
        TableStorageError::DuplicatePrimaryKey { table, columns } => refusal(
            "unique_violation",
            format!("primary key {:?} on `{table}` is already present", columns),
            "23505",
            [
                (String::from("table"), table),
                (String::from("columns"), columns.join(",")),
            ],
        ),
        TableStorageError::DuplicateUniqueKey {
            table,
            columns,
            name,
        } => refusal(
            "unique_violation",
            format!("unique key {:?} on `{table}` is already present", columns),
            "23505",
            [
                (String::from("table"), table),
                (String::from("columns"), columns.join(",")),
                (
                    String::from("constraint"),
                    name.unwrap_or_else(|| String::from("<anonymous>")),
                ),
            ],
        ),
        TableStorageError::NullPrimaryKey { table, columns } => refusal(
            "not_null_violation",
            format!("primary key {:?} on `{table}` cannot contain NULL", columns),
            "23502",
            [
                (String::from("table"), table),
                (String::from("columns"), columns.join(",")),
            ],
        ),
        other => refusal(
            "storage_error",
            other.to_string(),
            "XX000",
            [(String::from("error"), other.to_string())],
        ),
    }
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

fn refusal_from_row_error(error: RowBuildError) -> RefusalResult {
    match error {
        RowBuildError::Arity { expected, actual } => refusal(
            "row_arity",
            format!("row arity {actual} does not match expected {expected}"),
            "XX000",
            [
                (String::from("expected"), expected.to_string()),
                (String::from("actual"), actual.to_string()),
            ],
        ),
        RowBuildError::DefaultExpression { column } => refusal(
            "default_expression_unimplemented",
            format!("column `{column}` requires a default expression that is not implemented"),
            "0A000",
            [(String::from("column"), column)],
        ),
        RowBuildError::UnsupportedColumnType {
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
        RowBuildError::Coercion { column, source } => refusal(
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
enum RowBuildError {
    Arity {
        expected: usize,
        actual: usize,
    },
    DefaultExpression {
        column: String,
    },
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
