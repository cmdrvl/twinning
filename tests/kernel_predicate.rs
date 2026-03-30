#![forbid(unsafe_code)]

use twinning::{
    catalog::{TableCatalog, parse_postgres_schema},
    ir::{PredicateComparison, PredicateExpr, PredicateOperator, ScalarValue},
    kernel::{
        predicate::{PredicateError, TruthValue, evaluate_predicate, predicate_matches},
        value::KernelValue,
    },
};

#[test]
fn declared_predicate_operators_evaluate_over_kernel_rows() {
    let table = deals_table();
    let row = deal_row();

    let eq = PredicateExpr::Comparison(PredicateComparison {
        column: String::from("tenant_id"),
        operator: PredicateOperator::Eq,
        values: vec![ScalarValue::Text(String::from("tenant-a"))],
    });
    let in_list = PredicateExpr::Comparison(PredicateComparison {
        column: String::from("status"),
        operator: PredicateOperator::InList,
        values: vec![
            ScalarValue::Text(String::from("pending")),
            ScalarValue::Text(String::from("open")),
        ],
    });
    let between = PredicateExpr::Comparison(PredicateComparison {
        column: String::from("amount"),
        operator: PredicateOperator::Between,
        values: vec![ScalarValue::Integer(100), ScalarValue::Integer(200)],
    });
    let is_null = PredicateExpr::Comparison(PredicateComparison {
        column: String::from("deleted_at"),
        operator: PredicateOperator::IsNull,
        values: Vec::new(),
    });

    assert_eq!(
        evaluate_predicate(&table, &row, &eq).expect("evaluate eq"),
        TruthValue::True
    );
    assert!(predicate_matches(&table, &row, &eq).expect("predicate matches"));
    assert_eq!(
        evaluate_predicate(&table, &row, &in_list).expect("evaluate in-list"),
        TruthValue::True
    );
    assert_eq!(
        evaluate_predicate(&table, &row, &between).expect("evaluate between"),
        TruthValue::True
    );
    assert_eq!(
        evaluate_predicate(&table, &row, &is_null).expect("evaluate is-null"),
        TruthValue::True
    );
}

#[test]
fn three_valued_null_semantics_stay_explicit() {
    let table = deals_table();
    let row = deal_row();

    let null_eq = PredicateExpr::Comparison(PredicateComparison {
        column: String::from("deleted_at"),
        operator: PredicateOperator::Eq,
        values: vec![ScalarValue::Text(String::from("2026-03-30T00:00:00Z"))],
    });
    let false_and_unknown = PredicateExpr::Conjunction(vec![
        PredicateComparison {
            column: String::from("tenant_id"),
            operator: PredicateOperator::Eq,
            values: vec![ScalarValue::Text(String::from("tenant-b"))],
        },
        PredicateComparison {
            column: String::from("deleted_at"),
            operator: PredicateOperator::Eq,
            values: vec![ScalarValue::Text(String::from("2026-03-30T00:00:00Z"))],
        },
    ]);
    let true_or_unknown = PredicateExpr::Disjunction(vec![
        PredicateComparison {
            column: String::from("tenant_id"),
            operator: PredicateOperator::Eq,
            values: vec![ScalarValue::Text(String::from("tenant-a"))],
        },
        PredicateComparison {
            column: String::from("deleted_at"),
            operator: PredicateOperator::Eq,
            values: vec![ScalarValue::Text(String::from("2026-03-30T00:00:00Z"))],
        },
    ]);

    assert_eq!(
        evaluate_predicate(&table, &row, &null_eq).expect("evaluate null comparison"),
        TruthValue::Unknown
    );
    assert_eq!(
        evaluate_predicate(&table, &row, &false_and_unknown).expect("evaluate false and unknown"),
        TruthValue::False
    );
    assert_eq!(
        evaluate_predicate(&table, &row, &true_or_unknown).expect("evaluate true or unknown"),
        TruthValue::True
    );
}

#[test]
fn predicate_errors_stay_explicit_for_invalid_shapes_and_values() {
    let table = deals_table();
    let row = deal_row();

    let unknown_column = PredicateExpr::Comparison(PredicateComparison {
        column: String::from("missing_column"),
        operator: PredicateOperator::Eq,
        values: vec![ScalarValue::Text(String::from("x"))],
    });
    let invalid_arity = PredicateExpr::Comparison(PredicateComparison {
        column: String::from("tenant_id"),
        operator: PredicateOperator::Between,
        values: vec![ScalarValue::Text(String::from("tenant-a"))],
    });

    assert_eq!(
        evaluate_predicate(&table, &row, &unknown_column),
        Err(PredicateError::UnknownColumn {
            table: String::from("public.deals"),
            column: String::from("missing_column"),
        })
    );
    assert_eq!(
        evaluate_predicate(&table, &row, &invalid_arity),
        Err(PredicateError::OperatorArity {
            operator: String::from("between"),
            expected: String::from("two values"),
            actual: 1,
        })
    );

    let payload_table = payload_table();
    let payload_row = vec![
        KernelValue::Text(String::from("deal-001")),
        KernelValue::Json(serde_json::json!({ "tenant_id": "tenant-a" })),
    ];
    let json_predicate = PredicateExpr::Comparison(PredicateComparison {
        column: String::from("payload"),
        operator: PredicateOperator::Eq,
        values: vec![ScalarValue::Text(String::from("tenant-a"))],
    });

    assert_eq!(
        evaluate_predicate(&payload_table, &payload_row, &json_predicate),
        Err(PredicateError::UnsupportedValueKind {
            table: String::from("public.events"),
            column: String::from("payload"),
            kind: String::from("json"),
        })
    );
}

fn deals_table() -> TableCatalog {
    parse_postgres_schema(
        r#"
        CREATE TABLE public.deals (
            deal_id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            status TEXT,
            amount INTEGER,
            deleted_at TIMESTAMP
        );
        "#,
    )
    .expect("schema should parse")
    .table("public.deals")
    .expect("deals table")
    .clone()
}

fn payload_table() -> TableCatalog {
    parse_postgres_schema(
        r#"
        CREATE TABLE public.events (
            event_id TEXT PRIMARY KEY,
            payload JSON
        );
        "#,
    )
    .expect("schema should parse")
    .table("public.events")
    .expect("events table")
    .clone()
}

fn deal_row() -> Vec<KernelValue> {
    vec![
        KernelValue::Text(String::from("deal-001")),
        KernelValue::Text(String::from("tenant-a")),
        KernelValue::Text(String::from("open")),
        KernelValue::Integer(125),
        KernelValue::Null,
    ]
}
