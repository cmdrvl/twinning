#![forbid(unsafe_code)]

use twinning::{
    backend::{Backend, BaseSnapshotBackend},
    catalog::{Catalog, parse_postgres_schema},
    ir::{
        MutationKind, MutationOp, PredicateComparison, PredicateExpr, PredicateOperator,
        ScalarValue,
    },
    kernel::{mutation::execute_mutation, storage::TableStorage, value::KernelValue},
    result::KernelResult,
};

#[test]
fn update_by_predicate_rewrites_overlay_and_reports_rows_affected() {
    let (catalog, mut backend) = deals_backend();

    let result = execute_mutation(
        &catalog,
        &mut backend,
        &MutationOp {
            session_id: String::from("session-1"),
            table: String::from("public.deals"),
            kind: MutationKind::Update,
            columns: vec![String::from("status"), String::from("amount")],
            rows: vec![vec![
                ScalarValue::Text(String::from("closed")),
                ScalarValue::Integer(200),
            ]],
            conflict_target: None,
            update_columns: Vec::new(),
            predicate: Some(predicate_eq(
                "tenant_id",
                ScalarValue::Text(String::from("tenant-a")),
            )),
            returning: Vec::new(),
        },
    );

    let KernelResult::Mutation(result) = result else {
        panic!("expected mutation result");
    };
    assert_eq!(result.rows_affected, 2);

    let deals = backend
        .visible_table("public.deals")
        .expect("visible deals table");
    assert_eq!(
        deals
            .lookup_primary_key(&[KernelValue::Text(String::from("deal-001"))])
            .expect("lookup deal-001")
            .expect("deal-001 present")
            .values,
        vec![
            KernelValue::Text(String::from("deal-001")),
            KernelValue::Text(String::from("tenant-a")),
            KernelValue::Text(String::from("alpha-001")),
            KernelValue::Text(String::from("Alpha")),
            KernelValue::Text(String::from("closed")),
            KernelValue::Integer(200),
        ]
    );
    assert_eq!(
        deals
            .lookup_primary_key(&[KernelValue::Text(String::from("deal-002"))])
            .expect("lookup deal-002")
            .expect("deal-002 present")
            .values,
        vec![
            KernelValue::Text(String::from("deal-002")),
            KernelValue::Text(String::from("tenant-a")),
            KernelValue::Text(String::from("alpha-002")),
            KernelValue::Text(String::from("Beta")),
            KernelValue::Text(String::from("closed")),
            KernelValue::Integer(200),
        ]
    );
}

#[test]
fn delete_by_predicate_removes_rows_and_reports_rows_affected() {
    let (catalog, mut backend) = deals_backend();

    let result = execute_mutation(
        &catalog,
        &mut backend,
        &MutationOp {
            session_id: String::from("session-1"),
            table: String::from("public.deals"),
            kind: MutationKind::Delete,
            columns: Vec::new(),
            rows: Vec::new(),
            conflict_target: None,
            update_columns: Vec::new(),
            predicate: Some(predicate_eq(
                "status",
                ScalarValue::Text(String::from("closed")),
            )),
            returning: Vec::new(),
        },
    );

    let KernelResult::Mutation(result) = result else {
        panic!("expected mutation result");
    };
    assert_eq!(result.rows_affected, 1);

    let deals = backend
        .visible_table("public.deals")
        .expect("visible deals table");
    assert_eq!(deals.row_count(), 2);
    assert!(
        deals
            .lookup_primary_key(&[KernelValue::Text(String::from("deal-003"))])
            .expect("lookup deleted row")
            .is_none()
    );
}

#[test]
fn update_surfaces_constraint_sqlstate_from_write_kernel() {
    let (catalog, mut backend) = deals_backend();

    let result = execute_mutation(
        &catalog,
        &mut backend,
        &MutationOp {
            session_id: String::from("session-1"),
            table: String::from("public.deals"),
            kind: MutationKind::Update,
            columns: vec![String::from("amount")],
            rows: vec![vec![ScalarValue::Integer(-1)]],
            conflict_target: None,
            update_columns: Vec::new(),
            predicate: Some(predicate_eq(
                "deal_id",
                ScalarValue::Text(String::from("deal-001")),
            )),
            returning: Vec::new(),
        },
    );

    let KernelResult::Refusal(result) = result else {
        panic!("expected refusal result");
    };
    assert_eq!(result.code, "check_violation");
    assert_eq!(result.sqlstate, "23514");
}

#[test]
fn delete_referenced_parent_surfaces_foreign_key_sqlstate() {
    let (catalog, mut backend) = deals_backend();

    let result = execute_mutation(
        &catalog,
        &mut backend,
        &MutationOp {
            session_id: String::from("session-1"),
            table: String::from("public.tenants"),
            kind: MutationKind::Delete,
            columns: Vec::new(),
            rows: Vec::new(),
            conflict_target: None,
            update_columns: Vec::new(),
            predicate: Some(predicate_eq(
                "tenant_id",
                ScalarValue::Text(String::from("tenant-a")),
            )),
            returning: Vec::new(),
        },
    );

    let KernelResult::Refusal(result) = result else {
        panic!("expected refusal result");
    };
    assert_eq!(result.code, "foreign_key_violation");
    assert_eq!(result.sqlstate, "23503");
}

#[test]
fn broader_delete_shapes_stay_explicitly_refused() {
    let (catalog, mut backend) = deals_backend();

    let result = execute_mutation(
        &catalog,
        &mut backend,
        &MutationOp {
            session_id: String::from("session-1"),
            table: String::from("public.deals"),
            kind: MutationKind::Delete,
            columns: vec![String::from("status")],
            rows: vec![vec![ScalarValue::Text(String::from("closed"))]],
            conflict_target: None,
            update_columns: Vec::new(),
            predicate: Some(predicate_eq(
                "deal_id",
                ScalarValue::Text(String::from("deal-001")),
            )),
            returning: Vec::new(),
        },
    );

    let KernelResult::Refusal(result) = result else {
        panic!("expected refusal result");
    };
    assert_eq!(result.code, "unsupported_mutation_shape");
    assert_eq!(result.sqlstate, "0A000");
}

fn predicate_eq(column: &str, value: ScalarValue) -> PredicateExpr {
    PredicateExpr::Comparison(PredicateComparison {
        column: column.to_owned(),
        operator: PredicateOperator::Eq,
        values: vec![value],
    })
}

fn deals_backend() -> (Catalog, BaseSnapshotBackend) {
    let catalog = parse_postgres_schema(
        r#"
        CREATE TABLE public.tenants (
            tenant_id TEXT PRIMARY KEY
        );

        CREATE TABLE public.deals (
            deal_id TEXT PRIMARY KEY,
            tenant_id TEXT REFERENCES public.tenants (tenant_id),
            external_key TEXT,
            deal_name TEXT NOT NULL,
            status TEXT CHECK (status IN ('open', 'closed')),
            amount INTEGER CHECK (amount >= 0),
            CONSTRAINT deals_external_key_key UNIQUE (external_key)
        );
        "#,
    )
    .expect("schema should parse");

    let mut tenants = TableStorage::new(
        catalog
            .table("public.tenants")
            .expect("tenants table exists"),
    )
    .expect("tenant storage should build");
    tenants
        .insert_row(vec![KernelValue::Text(String::from("tenant-a"))])
        .expect("seed tenant-a");
    tenants
        .insert_row(vec![KernelValue::Text(String::from("tenant-b"))])
        .expect("seed tenant-b");

    let mut deals = TableStorage::new(catalog.table("public.deals").expect("deals table exists"))
        .expect("deal storage should build");
    deals
        .insert_row(vec![
            KernelValue::Text(String::from("deal-001")),
            KernelValue::Text(String::from("tenant-a")),
            KernelValue::Text(String::from("alpha-001")),
            KernelValue::Text(String::from("Alpha")),
            KernelValue::Null,
            KernelValue::Integer(100),
        ])
        .expect("seed deal-001");
    deals
        .insert_row(vec![
            KernelValue::Text(String::from("deal-002")),
            KernelValue::Text(String::from("tenant-a")),
            KernelValue::Text(String::from("alpha-002")),
            KernelValue::Text(String::from("Beta")),
            KernelValue::Text(String::from("open")),
            KernelValue::Integer(150),
        ])
        .expect("seed deal-002");
    deals
        .insert_row(vec![
            KernelValue::Text(String::from("deal-003")),
            KernelValue::Text(String::from("tenant-b")),
            KernelValue::Text(String::from("beta-001")),
            KernelValue::Text(String::from("Gamma")),
            KernelValue::Text(String::from("closed")),
            KernelValue::Integer(175),
        ])
        .expect("seed deal-003");

    let backend = BaseSnapshotBackend::new([tenants, deals]).expect("backend should build");
    (catalog, backend)
}
