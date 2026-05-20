use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use serde_json::{Value as JsonValue, json};

use crate::{
    backend::{Backend, BaseSnapshotBackend},
    catalog::{Catalog, ColumnCatalog, TableCatalog},
    ir::{MutationKind, MutationOp, ScalarValue},
    kernel::{mutation::execute_mutation, storage::TableStorage},
    refusal::{self, RefusalResult},
    result::{KernelResult, RefusalResult as KernelRefusal},
    seed::{
        SeedContractTarget, SeedDataRow, SeedField, parse_seed_jsonl, render_seed_contract_jsonl,
    },
};

const POSTGRES_TWIN: &str = "postgres";
const RELATION_TARGET: &str = "relation";
const SEED_SESSION_ID: &str = "__twinning_seed_jsonl__";

pub fn write_postgres_seed_contract(path: &Path, catalog: &Catalog) -> RefusalResult<()> {
    let rendered = render_seed_contract_jsonl(&postgres_seed_contract(catalog))
        .map_err(|error| Box::new(refusal::seed_jsonl(path, error.to_string())))?;
    std::fs::write(path, rendered).map_err(|error| Box::new(refusal::io_write(path, &error)))
}

pub fn load_postgres_seed_tables(
    catalog: &Catalog,
    path: &Path,
) -> RefusalResult<Vec<TableStorage>> {
    let bytes =
        std::fs::read_to_string(path).map_err(|error| Box::new(refusal::io_read(path, &error)))?;
    let document = parse_seed_jsonl(&bytes)
        .map_err(|error| Box::new(refusal::seed_jsonl(path, error.to_string())))?;
    let mut backend = BaseSnapshotBackend::new(empty_tables(catalog)?)
        .map_err(|error| Box::new(refusal::serialization(error.to_string())))?;

    for (index, row) in document.data_rows.iter().enumerate() {
        apply_seed_row(catalog, &mut backend, path, index + 1, row)?;
    }

    backend.promote_overlay_to_base();
    catalog
        .tables
        .iter()
        .map(|table| {
            backend
                .snapshot_base_table(&table.name)
                .map_err(|error| Box::new(refusal::serialization(error.to_string())))
        })
        .collect()
}

fn postgres_seed_contract(catalog: &Catalog) -> Vec<SeedContractTarget> {
    catalog
        .tables
        .iter()
        .map(|table| {
            let mut target = SeedContractTarget::new(
                POSTGRES_TWIN,
                RELATION_TARGET,
                table.name.clone(),
                table
                    .columns
                    .iter()
                    .map(|column| seed_field(table, column))
                    .collect(),
            );
            if let Some(primary_key) = &table.primary_key {
                target
                    .payload
                    .insert("primary_key".to_owned(), json!(&primary_key.columns));
            }
            if !table.unique_constraints.is_empty() {
                target.payload.insert(
                    "unique_constraints".to_owned(),
                    json!(
                        table
                            .unique_constraints
                            .iter()
                            .map(|constraint| json!({
                                "name": &constraint.name,
                                "columns": &constraint.columns
                            }))
                            .collect::<Vec<_>>()
                    ),
                );
            }
            if !table.foreign_keys.is_empty() {
                target.payload.insert(
                    "foreign_keys".to_owned(),
                    json!(
                        table
                            .foreign_keys
                            .iter()
                            .map(|foreign_key| json!({
                                "name": &foreign_key.name,
                                "columns": &foreign_key.columns,
                                "foreign_table": &foreign_key.foreign_table,
                                "referred_columns": &foreign_key.referred_columns
                            }))
                            .collect::<Vec<_>>()
                    ),
                );
            }
            target
        })
        .collect()
}

fn seed_field(table: &TableCatalog, column: &ColumnCatalog) -> SeedField {
    let mut field = SeedField::new(
        column.name.clone(),
        column.normalized_type.clone(),
        true,
        column.nullable,
    );
    field
        .payload
        .insert("declared_type".to_owned(), json!(&column.declared_type));
    if let Some(default_sql) = &column.default_sql {
        field
            .payload
            .insert("default_sql".to_owned(), json!(default_sql));
    }
    if table
        .primary_key
        .as_ref()
        .is_some_and(|primary_key| primary_key.columns.contains(&column.name))
    {
        field.payload.insert("primary_key".to_owned(), json!(true));
    }

    let unique_surfaces = table
        .unique_constraints
        .iter()
        .filter(|constraint| constraint.columns.contains(&column.name))
        .map(|constraint| {
            json!({
                "name": &constraint.name,
                "columns": &constraint.columns
            })
        })
        .collect::<Vec<_>>();
    if !unique_surfaces.is_empty() {
        field
            .payload
            .insert("unique_constraints".to_owned(), json!(unique_surfaces));
    }

    let references = table
        .foreign_keys
        .iter()
        .filter(|foreign_key| foreign_key.columns.contains(&column.name))
        .map(|foreign_key| {
            json!({
                "name": &foreign_key.name,
                "columns": &foreign_key.columns,
                "foreign_table": &foreign_key.foreign_table,
                "referred_columns": &foreign_key.referred_columns
            })
        })
        .collect::<Vec<_>>();
    if !references.is_empty() {
        field
            .payload
            .insert("references".to_owned(), json!(references));
    }

    field
}

fn empty_tables(catalog: &Catalog) -> RefusalResult<Vec<TableStorage>> {
    catalog
        .tables
        .iter()
        .map(|table| {
            TableStorage::new(table)
                .map_err(|error| Box::new(refusal::serialization(error.to_string())))
        })
        .collect()
}

fn apply_seed_row(
    catalog: &Catalog,
    backend: &mut BaseSnapshotBackend,
    path: &Path,
    row_number: usize,
    seed_row: &SeedDataRow,
) -> RefusalResult<()> {
    if seed_row.twin != POSTGRES_TWIN {
        return Err(Box::new(refusal::seed_jsonl(
            path,
            format!(
                "seed row {row_number} targets twin `{}`; expected `{POSTGRES_TWIN}`",
                seed_row.twin
            ),
        )));
    }
    if seed_row.target_kind != RELATION_TARGET {
        return Err(Box::new(refusal::seed_jsonl(
            path,
            format!(
                "seed row {row_number} targets kind `{}`; expected `{RELATION_TARGET}`",
                seed_row.target_kind
            ),
        )));
    }

    let table = catalog.table(&seed_row.target).ok_or_else(|| {
        Box::new(refusal::seed_jsonl(
            path,
            format!(
                "seed row {row_number} targets unknown relation `{}`",
                seed_row.target
            ),
        ))
    })?;
    validate_seed_row_shape(path, row_number, table, &seed_row.row)?;

    let mutation = MutationOp {
        session_id: SEED_SESSION_ID.to_owned(),
        table: table.name.clone(),
        kind: MutationKind::Insert,
        columns: table
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect(),
        rows: vec![
            table
                .columns
                .iter()
                .map(|column| json_to_scalar(&seed_row.row[&column.name]))
                .collect(),
        ],
        conflict_target: None,
        update_columns: Vec::new(),
        predicate: None,
        returning: Vec::new(),
    };

    match execute_mutation(catalog, backend, &mutation) {
        KernelResult::Mutation(_) => Ok(()),
        KernelResult::Refusal(refusal) => Err(Box::new(refusal::seed_jsonl(
            path,
            kernel_refusal_message(row_number, &seed_row.target, &refusal),
        ))),
        other => Err(Box::new(refusal::seed_jsonl(
            path,
            format!(
                "seed row {row_number} for `{}` produced unexpected kernel result `{other:?}`",
                seed_row.target
            ),
        ))),
    }
}

fn validate_seed_row_shape(
    path: &Path,
    row_number: usize,
    table: &TableCatalog,
    row: &BTreeMap<String, JsonValue>,
) -> RefusalResult<()> {
    let expected = table
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<BTreeSet<_>>();
    let actual = row.keys().map(String::as_str).collect::<BTreeSet<_>>();

    let missing = expected.difference(&actual).copied().collect::<Vec<_>>();
    let unknown = actual.difference(&expected).copied().collect::<Vec<_>>();

    if !missing.is_empty() || !unknown.is_empty() {
        return Err(Box::new(refusal::seed_jsonl(
            path,
            format!(
                "seed row {row_number} for `{}` must contain exactly the catalog columns; missing=[{}] unknown=[{}]",
                table.name,
                missing.join(","),
                unknown.join(",")
            ),
        )));
    }

    Ok(())
}

fn json_to_scalar(value: &JsonValue) -> ScalarValue {
    match value {
        JsonValue::Null => ScalarValue::Null,
        JsonValue::Bool(value) => ScalarValue::Boolean(*value),
        JsonValue::Number(value) => value
            .as_i64()
            .map(ScalarValue::Integer)
            .or_else(|| {
                value
                    .as_u64()
                    .and_then(|value| i64::try_from(value).ok())
                    .map(ScalarValue::Integer)
            })
            .unwrap_or_else(|| ScalarValue::Text(value.to_string())),
        JsonValue::String(value) => ScalarValue::Text(value.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => ScalarValue::Text(value.to_string()),
    }
}

fn kernel_refusal_message(row_number: usize, target: &str, refusal: &KernelRefusal) -> String {
    format!(
        "seed row {row_number} for `{target}` failed with {} (sqlstate {}): {}; detail={}",
        refusal.code,
        refusal.sqlstate,
        refusal.message,
        serde_json::to_string(&refusal.detail).unwrap_or_else(|_| "{}".to_owned())
    )
}

#[cfg(test)]
mod tests {
    use std::fs;

    use serde_json::json;
    use tempfile::tempdir;

    use crate::{catalog::parse_postgres_schema, kernel::value::KernelValue};

    use super::{load_postgres_seed_tables, write_postgres_seed_contract};

    #[test]
    fn postgres_contract_export_is_deterministic_jsonl() {
        let tempdir = tempdir().expect("tempdir");
        let output = tempdir.path().join("seed-contract.jsonl");
        let catalog = parse_postgres_schema(
            r#"
            CREATE TABLE public.tenants (
                tenant_id TEXT PRIMARY KEY,
                name TEXT NOT NULL
            );

            CREATE TABLE public.deals (
                deal_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL REFERENCES public.tenants (tenant_id),
                amount NUMERIC
            );
            "#,
        )
        .expect("schema");

        write_postgres_seed_contract(&output, &catalog).expect("contract export");

        let rendered = fs::read_to_string(&output).expect("read contract");
        assert_eq!(
            rendered,
            concat!(
                "{\"version\":\"twinning.seed-contract.v0\",\"kind\":\"target\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.deals\",\"fields\":[{\"name\":\"amount\",\"type\":\"numeric\",\"required\":true,\"nullable\":true,\"payload\":{\"declared_type\":\"NUMERIC\"}},{\"name\":\"deal_id\",\"type\":\"text\",\"required\":true,\"nullable\":false,\"payload\":{\"declared_type\":\"TEXT\",\"primary_key\":true}},{\"name\":\"tenant_id\",\"type\":\"text\",\"required\":true,\"nullable\":false,\"payload\":{\"declared_type\":\"TEXT\",\"references\":[{\"columns\":[\"tenant_id\"],\"foreign_table\":\"public.tenants\",\"name\":null,\"referred_columns\":[\"tenant_id\"]}]}}],\"payload\":{\"foreign_keys\":[{\"columns\":[\"tenant_id\"],\"foreign_table\":\"public.tenants\",\"name\":null,\"referred_columns\":[\"tenant_id\"]}],\"primary_key\":[\"deal_id\"]}}\n",
                "{\"version\":\"twinning.seed-contract.v0\",\"kind\":\"target\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.tenants\",\"fields\":[{\"name\":\"name\",\"type\":\"text\",\"required\":true,\"nullable\":false,\"payload\":{\"declared_type\":\"TEXT\"}},{\"name\":\"tenant_id\",\"type\":\"text\",\"required\":true,\"nullable\":false,\"payload\":{\"declared_type\":\"TEXT\",\"primary_key\":true}}],\"payload\":{\"primary_key\":[\"tenant_id\"]}}\n",
            )
        );
    }

    #[test]
    fn postgres_seed_import_preserves_jsonl_order_and_enforces_constraints() {
        let tempdir = tempdir().expect("tempdir");
        let seed_path = tempdir.path().join("seed.jsonl");
        let catalog = parse_postgres_schema(
            r#"
            CREATE TABLE public.tenants (
                tenant_id TEXT PRIMARY KEY,
                name TEXT NOT NULL
            );

            CREATE TABLE public.deals (
                deal_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL REFERENCES public.tenants (tenant_id),
                amount INTEGER
            );
            "#,
        )
        .expect("schema");
        fs::write(
            &seed_path,
            concat!(
                "{\"version\":\"twinning.seed-data.v0\",\"kind\":\"row\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.tenants\",\"row\":{\"tenant_id\":\"tenant-1\",\"name\":\"Acme\"}}\n",
                "{\"version\":\"twinning.seed-data.v0\",\"kind\":\"row\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.deals\",\"row\":{\"deal_id\":\"deal-1\",\"tenant_id\":\"tenant-1\",\"amount\":42}}\n",
            ),
        )
        .expect("write seed");

        let tables = load_postgres_seed_tables(&catalog, &seed_path).expect("seed import");
        let deals = tables
            .iter()
            .find(|table| table.table_name() == "public.deals")
            .expect("deals");
        let tenants = tables
            .iter()
            .find(|table| table.table_name() == "public.tenants")
            .expect("tenants");

        assert_eq!(tenants.row_count(), 1);
        assert_eq!(deals.row_count(), 1);
        let deal = deals.rows().next().expect("deal row");
        assert_eq!(deal.values[0], KernelValue::Text("deal-1".to_owned()));
        assert_eq!(deal.values[2], KernelValue::Integer(42));
    }

    #[test]
    fn postgres_seed_import_refuses_wrong_shape_and_constraint_failures() {
        let tempdir = tempdir().expect("tempdir");
        let catalog = parse_postgres_schema(
            r#"
            CREATE TABLE public.tenants (
                tenant_id TEXT PRIMARY KEY,
                name TEXT NOT NULL
            );
            "#,
        )
        .expect("schema");

        let missing_path = tempdir.path().join("missing.jsonl");
        fs::write(
            &missing_path,
            "{\"version\":\"twinning.seed-data.v0\",\"kind\":\"row\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.tenants\",\"row\":{\"tenant_id\":\"tenant-1\"}}\n",
        )
        .expect("write missing seed");
        let missing =
            load_postgres_seed_tables(&catalog, &missing_path).expect_err("missing column");
        let rendered = missing.render(true).expect("render refusal");
        assert!(rendered.contains("\"code\": \"E_SEED_JSONL\""));
        assert!(rendered.contains("missing=[name]"));

        let null_path = tempdir.path().join("null.jsonl");
        fs::write(
            &null_path,
            "{\"version\":\"twinning.seed-data.v0\",\"kind\":\"row\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.tenants\",\"row\":{\"tenant_id\":\"tenant-1\",\"name\":null}}\n",
        )
        .expect("write null seed");
        let null_refusal =
            load_postgres_seed_tables(&catalog, &null_path).expect_err("not null failure");
        let rendered = null_refusal.render(true).expect("render refusal");
        assert!(rendered.contains("not_null_violation"));
        assert!(rendered.contains("23502"));
    }

    #[test]
    fn postgres_seed_import_maps_catalog_and_constraint_refusals() {
        let tempdir = tempdir().expect("tempdir");
        let seed_path = tempdir.path().join("seed.jsonl");
        let catalog = parse_postgres_schema(
            r#"
            CREATE TABLE public.tenants (
                tenant_id TEXT PRIMARY KEY,
                name TEXT NOT NULL
            );

            CREATE TABLE public.deals (
                deal_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL REFERENCES public.tenants (tenant_id),
                external_key TEXT UNIQUE,
                amount INTEGER CHECK (amount >= 0)
            );
            "#,
        )
        .expect("schema");

        let cases = [
            (
                "unknown-table",
                "{\"version\":\"twinning.seed-data.v0\",\"kind\":\"row\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.missing\",\"row\":{\"id\":1}}\n",
                "unknown relation `public.missing`",
            ),
            (
                "unknown-column",
                "{\"version\":\"twinning.seed-data.v0\",\"kind\":\"row\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.tenants\",\"row\":{\"tenant_id\":\"tenant-1\",\"name\":\"Acme\",\"extra\":true}}\n",
                "unknown=[extra]",
            ),
            (
                "type-mismatch",
                "{\"version\":\"twinning.seed-data.v0\",\"kind\":\"row\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.deals\",\"row\":{\"deal_id\":\"deal-1\",\"tenant_id\":\"tenant-1\",\"external_key\":\"ext-1\",\"amount\":true}}\n",
                "invalid_text_representation",
            ),
            (
                "duplicate-primary-key",
                concat!(
                    "{\"version\":\"twinning.seed-data.v0\",\"kind\":\"row\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.tenants\",\"row\":{\"tenant_id\":\"tenant-1\",\"name\":\"Acme\"}}\n",
                    "{\"version\":\"twinning.seed-data.v0\",\"kind\":\"row\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.tenants\",\"row\":{\"tenant_id\":\"tenant-1\",\"name\":\"Duplicate\"}}\n",
                ),
                "unique_violation",
            ),
            (
                "foreign-key",
                "{\"version\":\"twinning.seed-data.v0\",\"kind\":\"row\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.deals\",\"row\":{\"deal_id\":\"deal-1\",\"tenant_id\":\"missing\",\"external_key\":\"ext-1\",\"amount\":1}}\n",
                "foreign_key_violation",
            ),
            (
                "check",
                concat!(
                    "{\"version\":\"twinning.seed-data.v0\",\"kind\":\"row\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.tenants\",\"row\":{\"tenant_id\":\"tenant-1\",\"name\":\"Acme\"}}\n",
                    "{\"version\":\"twinning.seed-data.v0\",\"kind\":\"row\",\"twin\":\"postgres\",\"target_kind\":\"relation\",\"target\":\"public.deals\",\"row\":{\"deal_id\":\"deal-1\",\"tenant_id\":\"tenant-1\",\"external_key\":\"ext-1\",\"amount\":-1}}\n",
                ),
                "check_violation",
            ),
        ];

        for (name, input, expected) in cases {
            fs::write(&seed_path, input).expect("write seed");
            let refusal = match load_postgres_seed_tables(&catalog, &seed_path) {
                Ok(_) => panic!("{name} should be refused"),
                Err(refusal) => refusal,
            };
            let rendered = refusal.render(true).expect("render refusal");
            assert!(
                rendered.contains(expected),
                "{name} refusal should contain {expected}; rendered={rendered}"
            );
        }
    }

    #[test]
    fn json_scalars_are_coerced_through_postgres_catalog_types() {
        let tempdir = tempdir().expect("tempdir");
        let seed_path = tempdir.path().join("seed.jsonl");
        let catalog = parse_postgres_schema(
            "CREATE TABLE public.seed_values (id INTEGER PRIMARY KEY, enabled BOOLEAN NOT NULL, ratio DOUBLE PRECISION NOT NULL, payload JSONB NOT NULL);",
        )
        .expect("schema");
        fs::write(
            &seed_path,
            format!(
                "{}\n",
                json!({
                    "version": "twinning.seed-data.v0",
                    "kind": "row",
                    "twin": "postgres",
                    "target_kind": "relation",
                    "target": "public.seed_values",
                    "row": {
                        "id": 1,
                        "enabled": true,
                        "ratio": 0.5,
                        "payload": { "source": "seed" }
                    }
                })
            ),
        )
        .expect("write seed");

        let tables = load_postgres_seed_tables(&catalog, &seed_path).expect("seed import");
        let values = tables
            .iter()
            .find(|table| table.table_name() == "public.seed_values")
            .expect("values table");
        let row = values.rows().next().expect("row");
        assert_eq!(row.values[0], KernelValue::Integer(1));
        assert_eq!(row.values[1], KernelValue::Boolean(true));
    }
}
