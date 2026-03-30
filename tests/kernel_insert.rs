#![forbid(unsafe_code)]

use twinning::{
    backend::{Backend, BaseSnapshotBackend},
    catalog::{Catalog, parse_postgres_schema},
    ir::{ConflictTarget, MutationKind, MutationOp, ScalarValue},
    kernel::{mutation::execute_insert, storage::TableStorage, value::KernelValue},
    result::{KernelResult, ResultTag},
};

#[test]
fn insert_updates_overlay_and_reports_rows_affected() {
    let (catalog, mut backend) = deals_backend();

    let result = execute_insert(
        &catalog,
        &mut backend,
        &MutationOp {
            session_id: String::from("session-1"),
            table: String::from("public.deals"),
            kind: MutationKind::Insert,
            columns: vec![
                String::from("deal_id"),
                String::from("tenant_id"),
                String::from("external_key"),
                String::from("deal_name"),
                String::from("status"),
                String::from("amount"),
            ],
            rows: vec![vec![
                ScalarValue::Text(String::from("deal-002")),
                ScalarValue::Text(String::from("tenant-a")),
                ScalarValue::Text(String::from("alpha-002")),
                ScalarValue::Text(String::from("Beta")),
                ScalarValue::Text(String::from("open")),
                ScalarValue::Integer(125),
            ]],
            conflict_target: None,
            update_columns: Vec::new(),
            predicate: None,
            returning: Vec::new(),
        },
    );

    let KernelResult::Mutation(result) = result else {
        panic!("expected mutation result");
    };
    assert_eq!(result.rows_affected, 1);
    assert!(result.returning_rows.is_empty());
    assert_eq!(
        backend
            .visible_table("public.deals")
            .expect("visible deals table")
            .row_count(),
        2
    );
}

#[test]
fn insert_returning_reports_selected_columns() {
    let (catalog, mut backend) = deals_backend();

    let result = execute_insert(
        &catalog,
        &mut backend,
        &MutationOp {
            session_id: String::from("session-1"),
            table: String::from("public.deals"),
            kind: MutationKind::Insert,
            columns: vec![
                String::from("deal_id"),
                String::from("tenant_id"),
                String::from("external_key"),
                String::from("deal_name"),
                String::from("status"),
                String::from("amount"),
            ],
            rows: vec![vec![
                ScalarValue::Text(String::from("deal-003")),
                ScalarValue::Text(String::from("tenant-a")),
                ScalarValue::Text(String::from("alpha-003")),
                ScalarValue::Text(String::from("Gamma")),
                ScalarValue::Null,
                ScalarValue::Integer(150),
            ]],
            conflict_target: None,
            update_columns: Vec::new(),
            predicate: None,
            returning: vec![String::from("deal_id"), String::from("deal_name")],
        },
    );

    let KernelResult::Mutation(result) = result else {
        panic!("expected mutation result");
    };
    assert_eq!(result.rows_affected, 1);
    assert_eq!(
        result.returning_rows,
        vec![vec![
            ScalarValue::Text(String::from("deal-003")),
            ScalarValue::Text(String::from("Gamma")),
        ]]
    );
}

#[test]
fn insert_surfaces_constraint_sqlstate_from_write_kernel() {
    let (catalog, mut backend) = deals_backend();

    let result = execute_insert(
        &catalog,
        &mut backend,
        &MutationOp {
            session_id: String::from("session-1"),
            table: String::from("public.deals"),
            kind: MutationKind::Insert,
            columns: vec![
                String::from("deal_id"),
                String::from("tenant_id"),
                String::from("external_key"),
                String::from("deal_name"),
                String::from("status"),
                String::from("amount"),
            ],
            rows: vec![vec![
                ScalarValue::Text(String::from("deal-004")),
                ScalarValue::Text(String::from("tenant-a")),
                ScalarValue::Text(String::from("alpha-001")),
                ScalarValue::Text(String::from("Duplicate Unique")),
                ScalarValue::Text(String::from("open")),
                ScalarValue::Integer(150),
            ]],
            conflict_target: None,
            update_columns: Vec::new(),
            predicate: None,
            returning: Vec::new(),
        },
    );

    let KernelResult::Refusal(result) = result else {
        panic!("expected refusal result");
    };
    assert_eq!(result.code, "unique_violation");
    assert_eq!(result.sqlstate, "23505");
}

#[test]
fn upsert_without_conflict_inserts_new_row_and_reports_upsert_tag() {
    let (catalog, mut backend) = deals_backend();

    let result = execute_insert(
        &catalog,
        &mut backend,
        &MutationOp {
            session_id: String::from("session-1"),
            table: String::from("public.deals"),
            kind: MutationKind::Upsert,
            columns: vec![
                String::from("deal_id"),
                String::from("tenant_id"),
                String::from("external_key"),
                String::from("deal_name"),
                String::from("status"),
                String::from("amount"),
            ],
            rows: vec![vec![
                ScalarValue::Text(String::from("deal-005")),
                ScalarValue::Text(String::from("tenant-a")),
                ScalarValue::Text(String::from("alpha-005")),
                ScalarValue::Text(String::from("Epsilon")),
                ScalarValue::Text(String::from("open")),
                ScalarValue::Integer(175),
            ]],
            conflict_target: Some(ConflictTarget::PrimaryKey),
            update_columns: vec![
                String::from("tenant_id"),
                String::from("external_key"),
                String::from("deal_name"),
                String::from("status"),
                String::from("amount"),
            ],
            predicate: None,
            returning: vec![String::from("deal_id")],
        },
    );

    let KernelResult::Mutation(result) = result else {
        panic!("expected mutation result");
    };
    assert_eq!(result.tag, ResultTag::Upsert);
    assert_eq!(result.rows_affected, 1);
    assert_eq!(
        result.returning_rows,
        vec![vec![ScalarValue::Text(String::from("deal-005"))]]
    );
    assert_eq!(
        backend
            .visible_table("public.deals")
            .expect("visible deals table")
            .row_count(),
        2
    );
}

#[test]
fn upsert_rewrites_conflicting_primary_key_row() {
    let (catalog, mut backend) = deals_backend();

    let result = execute_insert(
        &catalog,
        &mut backend,
        &MutationOp {
            session_id: String::from("session-1"),
            table: String::from("public.deals"),
            kind: MutationKind::Upsert,
            columns: vec![
                String::from("deal_id"),
                String::from("tenant_id"),
                String::from("external_key"),
                String::from("deal_name"),
                String::from("status"),
                String::from("amount"),
            ],
            rows: vec![vec![
                ScalarValue::Text(String::from("deal-001")),
                ScalarValue::Text(String::from("tenant-a")),
                ScalarValue::Text(String::from("alpha-009")),
                ScalarValue::Text(String::from("Alpha Updated")),
                ScalarValue::Text(String::from("closed")),
                ScalarValue::Integer(225),
            ]],
            conflict_target: Some(ConflictTarget::PrimaryKey),
            update_columns: vec![
                String::from("tenant_id"),
                String::from("external_key"),
                String::from("deal_name"),
                String::from("status"),
                String::from("amount"),
            ],
            predicate: None,
            returning: vec![String::from("deal_id"), String::from("deal_name")],
        },
    );

    let KernelResult::Mutation(result) = result else {
        panic!("expected mutation result");
    };
    assert_eq!(result.tag, ResultTag::Upsert);
    assert_eq!(result.rows_affected, 1);
    assert_eq!(
        result.returning_rows,
        vec![vec![
            ScalarValue::Text(String::from("deal-001")),
            ScalarValue::Text(String::from("Alpha Updated")),
        ]]
    );

    let deals = backend
        .visible_table("public.deals")
        .expect("visible deals table");
    assert_eq!(deals.row_count(), 1);
    assert_eq!(
        deals
            .lookup_primary_key(&[KernelValue::Text(String::from("deal-001"))])
            .expect("lookup primary key")
            .expect("rewritten row")
            .values,
        vec![
            KernelValue::Text(String::from("deal-001")),
            KernelValue::Text(String::from("tenant-a")),
            KernelValue::Text(String::from("alpha-009")),
            KernelValue::Text(String::from("Alpha Updated")),
            KernelValue::Text(String::from("closed")),
            KernelValue::Integer(225),
        ]
    );
}

#[test]
fn upsert_rewrites_conflicting_named_unique_row() {
    let (catalog, mut backend) = deals_backend();

    let result = execute_insert(
        &catalog,
        &mut backend,
        &MutationOp {
            session_id: String::from("session-1"),
            table: String::from("public.deals"),
            kind: MutationKind::Upsert,
            columns: vec![
                String::from("deal_id"),
                String::from("tenant_id"),
                String::from("external_key"),
                String::from("deal_name"),
                String::from("status"),
                String::from("amount"),
            ],
            rows: vec![vec![
                ScalarValue::Text(String::from("deal-002")),
                ScalarValue::Text(String::from("tenant-a")),
                ScalarValue::Text(String::from("alpha-001")),
                ScalarValue::Text(String::from("Alpha Unique Rewrite")),
                ScalarValue::Text(String::from("open")),
                ScalarValue::Integer(140),
            ]],
            conflict_target: Some(ConflictTarget::NamedConstraint(String::from(
                "deals_external_key_key",
            ))),
            update_columns: vec![String::from("deal_id"), String::from("deal_name")],
            predicate: None,
            returning: vec![String::from("deal_id"), String::from("external_key")],
        },
    );

    let KernelResult::Mutation(result) = result else {
        panic!("expected mutation result");
    };
    assert_eq!(result.tag, ResultTag::Upsert);
    assert_eq!(
        result.returning_rows,
        vec![vec![
            ScalarValue::Text(String::from("deal-002")),
            ScalarValue::Text(String::from("alpha-001")),
        ]]
    );

    let deals = backend
        .visible_table("public.deals")
        .expect("visible deals table");
    assert_eq!(deals.row_count(), 1);
    assert!(
        deals
            .lookup_primary_key(&[KernelValue::Text(String::from("deal-001"))])
            .expect("lookup old primary key")
            .is_none()
    );
    assert_eq!(
        deals
            .lookup_primary_key(&[KernelValue::Text(String::from("deal-002"))])
            .expect("lookup rewritten primary key")
            .expect("rewritten unique row")
            .values,
        vec![
            KernelValue::Text(String::from("deal-002")),
            KernelValue::Text(String::from("tenant-a")),
            KernelValue::Text(String::from("alpha-001")),
            KernelValue::Text(String::from("Alpha Unique Rewrite")),
            KernelValue::Null,
            KernelValue::Integer(100),
        ]
    );
}

#[test]
fn upsert_preserves_omitted_non_target_columns_from_conflicting_row() {
    let (catalog, mut backend) = deals_backend();

    let result = execute_insert(
        &catalog,
        &mut backend,
        &MutationOp {
            session_id: String::from("session-1"),
            table: String::from("public.deals"),
            kind: MutationKind::Upsert,
            columns: vec![
                String::from("deal_id"),
                String::from("tenant_id"),
                String::from("external_key"),
                String::from("deal_name"),
                String::from("status"),
                String::from("amount"),
            ],
            rows: vec![vec![
                ScalarValue::Text(String::from("deal-001")),
                ScalarValue::Text(String::from("tenant-b")),
                ScalarValue::Text(String::from("alpha-009")),
                ScalarValue::Text(String::from("Alpha Updated")),
                ScalarValue::Text(String::from("closed")),
                ScalarValue::Integer(225),
            ]],
            conflict_target: Some(ConflictTarget::PrimaryKey),
            update_columns: vec![String::from("external_key"), String::from("deal_name")],
            predicate: None,
            returning: vec![
                String::from("tenant_id"),
                String::from("external_key"),
                String::from("deal_name"),
                String::from("status"),
                String::from("amount"),
            ],
        },
    );

    let KernelResult::Mutation(result) = result else {
        panic!("expected mutation result");
    };
    assert_eq!(
        result.returning_rows,
        vec![vec![
            ScalarValue::Text(String::from("tenant-a")),
            ScalarValue::Text(String::from("alpha-009")),
            ScalarValue::Text(String::from("Alpha Updated")),
            ScalarValue::Null,
            ScalarValue::Integer(100),
        ]]
    );

    let deals = backend
        .visible_table("public.deals")
        .expect("visible deals table");
    assert_eq!(
        deals
            .lookup_primary_key(&[KernelValue::Text(String::from("deal-001"))])
            .expect("lookup rewritten primary key")
            .expect("rewritten row")
            .values,
        vec![
            KernelValue::Text(String::from("deal-001")),
            KernelValue::Text(String::from("tenant-a")),
            KernelValue::Text(String::from("alpha-009")),
            KernelValue::Text(String::from("Alpha Updated")),
            KernelValue::Null,
            KernelValue::Integer(100),
        ]
    );
}

#[test]
fn upsert_refuses_unknown_conflict_target_surface() {
    let (catalog, mut backend) = deals_backend();

    let result = execute_insert(
        &catalog,
        &mut backend,
        &MutationOp {
            session_id: String::from("session-1"),
            table: String::from("public.deals"),
            kind: MutationKind::Upsert,
            columns: vec![
                String::from("deal_id"),
                String::from("tenant_id"),
                String::from("external_key"),
                String::from("deal_name"),
                String::from("status"),
                String::from("amount"),
            ],
            rows: vec![vec![
                ScalarValue::Text(String::from("deal-010")),
                ScalarValue::Text(String::from("tenant-a")),
                ScalarValue::Text(String::from("alpha-010")),
                ScalarValue::Text(String::from("Unsupported Target")),
                ScalarValue::Text(String::from("open")),
                ScalarValue::Integer(190),
            ]],
            conflict_target: Some(ConflictTarget::Columns(vec![String::from("amount")])),
            update_columns: vec![
                String::from("deal_id"),
                String::from("tenant_id"),
                String::from("external_key"),
                String::from("deal_name"),
                String::from("status"),
            ],
            predicate: None,
            returning: Vec::new(),
        },
    );

    let KernelResult::Refusal(result) = result else {
        panic!("expected refusal result");
    };
    assert_eq!(result.code, "unknown_conflict_target");
    assert_eq!(result.sqlstate, "0A000");
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
        .expect("seed tenant");

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
        .expect("seed deal");

    let backend = BaseSnapshotBackend::new([tenants, deals]).expect("backend should build");
    (catalog, backend)
}
