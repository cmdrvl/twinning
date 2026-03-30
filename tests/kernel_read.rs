#![forbid(unsafe_code)]

use twinning::{
    backend::{Backend, BaseSnapshotBackend},
    catalog::{Catalog, parse_postgres_schema},
    ir::{
        AggregateKind, AggregateSpec, PredicateComparison, PredicateExpr, PredicateOperator,
        ReadOp, ReadShape, ScalarValue,
    },
    kernel::{read::execute_read, storage::TableStorage, value::KernelValue},
    result::KernelResult,
};

#[test]
fn point_lookup_reads_committed_primary_key_row() {
    let (catalog, backend) = deals_backend();

    let result = execute_read(
        &catalog,
        &backend,
        &ReadOp {
            session_id: String::from("session-1"),
            table: String::from("public.deals"),
            shape: ReadShape::PointLookup,
            projection: vec![String::from("deal_id"), String::from("deal_name")],
            predicate: Some(PredicateExpr::Comparison(PredicateComparison {
                column: String::from("deal_id"),
                operator: PredicateOperator::Eq,
                values: vec![ScalarValue::Text(String::from("deal-001"))],
            })),
            aggregate: AggregateSpec::default(),
            group_by: Vec::new(),
            limit: None,
        },
    );

    let KernelResult::Read(result) = result else {
        panic!("expected read result");
    };
    assert_eq!(
        result.columns,
        vec![String::from("deal_id"), String::from("deal_name")]
    );
    assert_eq!(
        result.rows,
        vec![vec![
            ScalarValue::Text(String::from("deal-001")),
            ScalarValue::Text(String::from("Alpha")),
        ]]
    );
}

#[test]
fn filtered_scan_uses_committed_rows_and_keeps_deterministic_order() {
    let (catalog, mut backend) = deals_backend();
    let mut overlay = backend
        .snapshot_base_table("public.deals")
        .expect("snapshot deals");
    overlay
        .insert_row(vec![
            KernelValue::Text(String::from("deal-999")),
            KernelValue::Text(String::from("tenant-a")),
            KernelValue::Text(String::from("overlay-999")),
            KernelValue::Text(String::from("Overlay")),
            KernelValue::Text(String::from("open")),
            KernelValue::Integer(999),
        ])
        .expect("insert overlay row");
    backend.write_overlay_table(overlay).expect("write overlay");

    let result = execute_read(
        &catalog,
        &backend,
        &ReadOp {
            session_id: String::from("session-1"),
            table: String::from("public.deals"),
            shape: ReadShape::FilteredScan,
            projection: vec![String::from("deal_id"), String::from("tenant_id")],
            predicate: Some(PredicateExpr::Comparison(PredicateComparison {
                column: String::from("tenant_id"),
                operator: PredicateOperator::Eq,
                values: vec![ScalarValue::Text(String::from("tenant-a"))],
            })),
            aggregate: AggregateSpec::default(),
            group_by: Vec::new(),
            limit: Some(2),
        },
    );

    let KernelResult::Read(result) = result else {
        panic!("expected read result");
    };
    assert_eq!(
        result.columns,
        vec![String::from("deal_id"), String::from("tenant_id")]
    );
    assert_eq!(
        result.rows,
        vec![
            vec![
                ScalarValue::Text(String::from("deal-001")),
                ScalarValue::Text(String::from("tenant-a")),
            ],
            vec![
                ScalarValue::Text(String::from("deal-002")),
                ScalarValue::Text(String::from("tenant-a")),
            ],
        ]
    );
}

#[test]
fn aggregate_count_reads_committed_rows_with_and_without_group_by() {
    let (catalog, backend) = deals_backend();

    let count_all = execute_read(
        &catalog,
        &backend,
        &ReadOp {
            session_id: String::from("session-1"),
            table: String::from("public.deals"),
            shape: ReadShape::AggregateScan,
            projection: Vec::new(),
            predicate: Some(PredicateExpr::Comparison(PredicateComparison {
                column: String::from("tenant_id"),
                operator: PredicateOperator::Eq,
                values: vec![ScalarValue::Text(String::from("tenant-a"))],
            })),
            aggregate: AggregateSpec {
                kind: AggregateKind::Count,
                column: None,
                alias: Some(String::from("row_count")),
            },
            group_by: Vec::new(),
            limit: None,
        },
    );

    let KernelResult::Read(count_all) = count_all else {
        panic!("expected count result");
    };
    assert_eq!(count_all.columns, vec![String::from("row_count")]);
    assert_eq!(count_all.rows, vec![vec![ScalarValue::Integer(2)]]);

    let group_count = execute_read(
        &catalog,
        &backend,
        &ReadOp {
            session_id: String::from("session-1"),
            table: String::from("public.deals"),
            shape: ReadShape::AggregateScan,
            projection: vec![String::from("tenant_id")],
            predicate: None,
            aggregate: AggregateSpec {
                kind: AggregateKind::Count,
                column: Some(String::from("status")),
                alias: Some(String::from("status_count")),
            },
            group_by: vec![String::from("tenant_id")],
            limit: None,
        },
    );

    let KernelResult::Read(group_count) = group_count else {
        panic!("expected grouped count result");
    };
    assert_eq!(
        group_count.columns,
        vec![String::from("tenant_id"), String::from("status_count")]
    );
    assert_eq!(
        group_count.rows,
        vec![
            vec![
                ScalarValue::Text(String::from("tenant-a")),
                ScalarValue::Integer(1),
            ],
            vec![
                ScalarValue::Text(String::from("tenant-b")),
                ScalarValue::Integer(1),
            ],
        ]
    );
}

#[test]
fn broader_aggregate_shapes_stay_explicitly_refused() {
    let (catalog, backend) = deals_backend();

    let unsupported_sum = execute_read(
        &catalog,
        &backend,
        &ReadOp {
            session_id: String::from("session-1"),
            table: String::from("public.deals"),
            shape: ReadShape::AggregateScan,
            projection: Vec::new(),
            predicate: None,
            aggregate: AggregateSpec {
                kind: AggregateKind::Sum,
                column: Some(String::from("amount")),
                alias: Some(String::from("amount_sum")),
            },
            group_by: Vec::new(),
            limit: None,
        },
    );

    let KernelResult::Refusal(unsupported_sum) = unsupported_sum else {
        panic!("expected refusal result");
    };
    assert_eq!(unsupported_sum.code, "unsupported_read_shape");
    assert_eq!(
        unsupported_sum.detail.get("aggregate"),
        Some(&String::from("sum"))
    );
}

fn deals_backend() -> (Catalog, BaseSnapshotBackend) {
    let catalog = parse_postgres_schema(
        r#"
        CREATE TABLE public.deals (
            deal_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            external_key TEXT UNIQUE,
            deal_name TEXT NOT NULL,
            status TEXT,
            amount INTEGER
        );
        "#,
    )
    .expect("schema should parse");

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
        .expect("seed deal 1");
    deals
        .insert_row(vec![
            KernelValue::Text(String::from("deal-002")),
            KernelValue::Text(String::from("tenant-a")),
            KernelValue::Text(String::from("alpha-002")),
            KernelValue::Text(String::from("Beta")),
            KernelValue::Text(String::from("open")),
            KernelValue::Integer(125),
        ])
        .expect("seed deal 2");
    deals
        .insert_row(vec![
            KernelValue::Text(String::from("deal-003")),
            KernelValue::Text(String::from("tenant-b")),
            KernelValue::Text(String::from("alpha-003")),
            KernelValue::Text(String::from("Gamma")),
            KernelValue::Text(String::from("closed")),
            KernelValue::Integer(90),
        ])
        .expect("seed deal 3");

    let backend = BaseSnapshotBackend::new([deals]).expect("backend should build");
    (catalog, backend)
}
