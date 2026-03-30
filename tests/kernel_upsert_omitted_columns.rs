#![forbid(unsafe_code)]

use twinning::{
    backend::{Backend, BaseSnapshotBackend},
    catalog::{Catalog, parse_postgres_schema},
    ir::{ConflictTarget, MutationKind, Operation, ScalarValue, normalize_mutation_sql},
    kernel::{mutation::execute_insert, storage::TableStorage, value::KernelValue},
    result::{KernelResult, ResultTag},
};

const WRITE_CORPUS_SCHEMA: &str = include_str!("fixtures/differential/write_corpus/schema.sql");
const UPSERT_PRIMARY_KEY_SQL: &str = "INSERT INTO public.deals (deal_id, tenant_id, external_key, deal_name) VALUES ('deal-001', 'tenant-a', 'ext-001-rewrite', 'Alpha Updated') ON CONFLICT (deal_id) DO UPDATE SET external_key = EXCLUDED.external_key, deal_name = EXCLUDED.deal_name";

#[test]
fn write_corpus_primary_key_upsert_normalizes_and_executes_with_omitted_tenant_update() {
    let (catalog, mut backend) = write_corpus_backend();

    let operation = normalize_mutation_sql(&catalog, "session-1", UPSERT_PRIMARY_KEY_SQL);
    let Operation::Mutation(mutation) = operation else {
        panic!("expected mutation operation, got {operation:?}");
    };

    assert_eq!(mutation.kind, MutationKind::Upsert);
    assert_eq!(mutation.table, "public.deals");
    assert_eq!(mutation.conflict_target, Some(ConflictTarget::PrimaryKey));
    assert_eq!(
        mutation.columns,
        vec![
            String::from("deal_id"),
            String::from("tenant_id"),
            String::from("external_key"),
            String::from("deal_name"),
        ]
    );
    assert_eq!(
        mutation.rows,
        vec![vec![
            ScalarValue::Text(String::from("deal-001")),
            ScalarValue::Text(String::from("tenant-a")),
            ScalarValue::Text(String::from("ext-001-rewrite")),
            ScalarValue::Text(String::from("Alpha Updated")),
        ]]
    );
    assert_eq!(
        mutation.update_columns,
        vec![String::from("deal_name"), String::from("external_key")]
    );

    let result = execute_insert(&catalog, &mut backend, &mutation);
    let KernelResult::Mutation(result) = result else {
        panic!("expected mutation result, got {result:?}");
    };

    assert_eq!(result.tag, ResultTag::Upsert);
    assert_eq!(result.rows_affected, 1);
    assert!(result.returning_rows.is_empty());

    let deals = backend
        .visible_table("public.deals")
        .expect("visible deals table");
    assert_eq!(deals.row_count(), 1);
    assert_eq!(
        deals
            .lookup_primary_key(&[KernelValue::Text(String::from("deal-001"))])
            .expect("lookup seeded primary key")
            .expect("updated row")
            .values,
        vec![
            KernelValue::Text(String::from("deal-001")),
            KernelValue::Text(String::from("tenant-a")),
            KernelValue::Text(String::from("ext-001-rewrite")),
            KernelValue::Text(String::from("Alpha Updated")),
        ]
    );
}

fn write_corpus_backend() -> (Catalog, BaseSnapshotBackend) {
    let catalog = parse_postgres_schema(WRITE_CORPUS_SCHEMA).expect("schema should parse");

    let mut tenants = TableStorage::new(
        catalog
            .table("public.tenants")
            .expect("tenants table exists"),
    )
    .expect("tenant storage should build");
    tenants
        .insert_row(vec![
            KernelValue::Text(String::from("tenant-a")),
            KernelValue::Text(String::from("Tenant A")),
        ])
        .expect("seed tenant");

    let mut deals = TableStorage::new(catalog.table("public.deals").expect("deals table exists"))
        .expect("deals storage should build");
    deals
        .insert_row(vec![
            KernelValue::Text(String::from("deal-001")),
            KernelValue::Text(String::from("tenant-a")),
            KernelValue::Text(String::from("ext-001")),
            KernelValue::Text(String::from("Alpha")),
        ])
        .expect("seed deal");

    let backend = BaseSnapshotBackend::new([tenants, deals]).expect("backend should build");
    (catalog, backend)
}
