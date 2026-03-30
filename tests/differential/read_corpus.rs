use std::fs;

use serde::Deserialize;
use serde_json::{Value, json};
use twinning::{
    backend::{Backend, BaseSnapshotBackend},
    catalog::{Catalog, parse_postgres_schema},
    ir::{Operation, ScalarValue, normalize_mutation_sql, normalize_read_sql},
    kernel::{mutation::execute_insert, read::execute_read, storage::TableStorage},
    result::KernelResult,
};

use super::{
    assertions::{
        DifferentialRow, OutcomeClass, assert_outcome_class, assert_refusal_classification,
        assert_rowset_eq, assert_skip_classification, assert_sqlstate,
    },
    runner::{CorpusKind, DifferentialRunner},
};

#[test]
fn read_corpus() {
    let runner = DifferentialRunner::prepare(CorpusKind::Read).expect("prepare differential run");
    let corpus = fs::read_to_string(runner.fixture().corpus_path()).expect("read read corpus");
    let corpus: ReadCorpusFixture =
        serde_json::from_str(&corpus).expect("parse read corpus fixture");
    let schema = fs::read_to_string(runner.fixture().schema_path()).expect("read read schema");
    let catalog = parse_postgres_schema(&schema).expect("parse read schema");

    assert_eq!(runner.fixture().id(), "read_corpus");
    assert!(runner.fixture().fixture_dir().exists());
    assert!(runner.scratch_dir().path().exists());

    for case in &corpus.cases {
        let mut backend = backend_for_catalog(&catalog);
        for (seed_index, seed_sql) in case.seed_sql.iter().enumerate() {
            seed_backend(
                &catalog,
                &mut backend,
                format!("{}-seed-{}", case.id, seed_index + 1).as_str(),
                seed_sql,
            );
        }
        let committed_backend = committed_backend(&catalog, &backend);

        match case.id.as_str() {
            "select_by_pk" | "select_filtered_scan" | "select_is_null" => {
                let TwinReadOutcome::Success { columns, rows } =
                    execute_read_sql(&catalog, &committed_backend, case.id.as_str(), &case.sql)
                else {
                    panic!("read corpus case `{}` unexpectedly refused", case.id);
                };
                assert_outcome_class(
                    case.expected.outcome_class(),
                    OutcomeClass::Success,
                    format!("read corpus {} classification", case.id).as_str(),
                );
                assert_rowset_eq(
                    columns.as_slice(),
                    rows.as_slice(),
                    case.expected.columns(),
                    case.expected.rows(),
                    format!("read corpus {} rowset", case.id).as_str(),
                );
            }
            "unsupported_select_for_update" => {
                let TwinReadOutcome::Refusal { sqlstate } =
                    execute_read_sql(&catalog, &committed_backend, case.id.as_str(), &case.sql)
                else {
                    panic!("read corpus case `{}` unexpectedly succeeded", case.id);
                };
                assert_refusal_classification(
                    case.expected.outcome_class(),
                    "read corpus unsupported_select_for_update classification",
                );
                assert_sqlstate(
                    Some(sqlstate.as_str()),
                    "0A000",
                    "read corpus unsupported_select_for_update sqlstate",
                );
            }
            "aggregate_count_skip" => {
                assert_skip_classification(
                    case.expected.outcome_class(),
                    "read corpus aggregate_count_skip classification",
                );
            }
            other => panic!("unexpected read corpus case `{other}`"),
        }
    }
}

#[test]
fn read_corpus_fixture_is_checked_in_and_wired() {
    let runner = DifferentialRunner::prepare(CorpusKind::Read).expect("prepare differential run");
    let corpus = fs::read_to_string(runner.fixture().corpus_path()).expect("read read corpus");
    let corpus: ReadCorpusFixture =
        serde_json::from_str(&corpus).expect("parse read corpus fixture");

    assert_eq!(runner.fixture().id(), "read_corpus");
    assert!(runner.fixture().fixture_dir().exists());
    assert!(runner.fixture().schema_path().exists());
    assert!(runner.fixture().corpus_path().exists());
    assert_eq!(corpus.version, "twinning.differential.read-corpus.v0");
    assert_eq!(corpus.cases.len(), 5);

    let case_ids = corpus
        .cases
        .iter()
        .map(|case| case.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        case_ids,
        vec![
            "select_by_pk",
            "select_filtered_scan",
            "select_is_null",
            "unsupported_select_for_update",
            "aggregate_count_skip",
        ]
    );

    for case in &corpus.cases {
        assert!(
            !case.sql.trim().is_empty(),
            "case `{}` must declare SQL",
            case.id
        );
        assert!(
            !case.seed_sql.is_empty(),
            "case `{}` should declare seed SQL for deterministic Postgres setup",
            case.id
        );

        match case.id.as_str() {
            "select_by_pk" => {
                assert_outcome_class(
                    case.expected.outcome_class(),
                    OutcomeClass::Success,
                    "read corpus select_by_pk classification",
                );
                assert_rowset_eq(
                    case.expected.columns(),
                    case.expected.rows(),
                    ["deal_id", "deal_name"],
                    &[row([
                        ("deal_id", json!("deal-001")),
                        ("deal_name", json!("Alpha")),
                    ])],
                    "read corpus select_by_pk rowset",
                );
            }
            "select_filtered_scan" => {
                assert_outcome_class(
                    case.expected.outcome_class(),
                    OutcomeClass::Success,
                    "read corpus select_filtered_scan classification",
                );
                assert_rowset_eq(
                    case.expected.columns(),
                    case.expected.rows(),
                    ["deal_id", "tenant_id"],
                    &[row([
                        ("deal_id", json!("deal-003")),
                        ("tenant_id", json!("tenant-b")),
                    ])],
                    "read corpus select_filtered_scan rowset",
                );
            }
            "select_is_null" => {
                assert_outcome_class(
                    case.expected.outcome_class(),
                    OutcomeClass::Success,
                    "read corpus select_is_null classification",
                );
                assert_rowset_eq(
                    case.expected.columns(),
                    case.expected.rows(),
                    ["deal_id"],
                    &[row([("deal_id", json!("deal-001"))])],
                    "read corpus select_is_null rowset",
                );
            }
            "unsupported_select_for_update" => {
                assert_refusal_classification(
                    case.expected.outcome_class(),
                    "read corpus unsupported_select_for_update classification",
                );
                assert_sqlstate(
                    case.expected.sqlstate.as_deref(),
                    "0A000",
                    "read corpus unsupported_select_for_update sqlstate",
                );
            }
            "aggregate_count_skip" => {
                assert_skip_classification(
                    case.expected.outcome_class(),
                    "read corpus aggregate_count_skip classification",
                );
            }
            other => panic!("unexpected read corpus case `{other}`"),
        }
    }
}

#[derive(Debug, Deserialize)]
struct ReadCorpusFixture {
    version: String,
    cases: Vec<ReadCorpusCase>,
}

#[derive(Debug, Deserialize)]
struct ReadCorpusCase {
    id: String,
    #[allow(dead_code)]
    description: String,
    seed_sql: Vec<String>,
    sql: String,
    expected: ReadCorpusExpectation,
}

#[derive(Debug, Deserialize)]
struct ReadCorpusExpectation {
    outcome_class: String,
    columns: Option<Vec<String>>,
    rows: Option<Vec<DifferentialRow>>,
    sqlstate: Option<String>,
}

impl ReadCorpusExpectation {
    fn outcome_class(&self) -> OutcomeClass {
        match self.outcome_class.as_str() {
            "success" => OutcomeClass::Success,
            "refusal" => OutcomeClass::Refusal,
            "skip" => OutcomeClass::Skip,
            other => panic!("unexpected differential outcome class `{other}`"),
        }
    }

    fn columns(&self) -> &[String] {
        self.columns
            .as_deref()
            .expect("success read cases must declare expected columns")
    }

    fn rows(&self) -> &[DifferentialRow] {
        self.rows
            .as_deref()
            .expect("success read cases must declare expected rows")
    }
}

enum TwinReadOutcome {
    Success {
        columns: Vec<String>,
        rows: Vec<DifferentialRow>,
    },
    Refusal {
        sqlstate: String,
    },
}

fn backend_for_catalog(catalog: &Catalog) -> BaseSnapshotBackend {
    let tables = catalog
        .tables
        .iter()
        .map(|table| TableStorage::new(table).expect("build table storage"))
        .collect::<Vec<_>>();
    BaseSnapshotBackend::new(tables).expect("build backend")
}

fn committed_backend(catalog: &Catalog, backend: &BaseSnapshotBackend) -> BaseSnapshotBackend {
    let tables = catalog
        .tables
        .iter()
        .map(|table| {
            backend
                .visible_table(&table.name)
                .cloned()
                .expect("visible table should exist")
        })
        .collect::<Vec<_>>();
    BaseSnapshotBackend::new(tables).expect("build committed backend")
}

fn seed_backend(catalog: &Catalog, backend: &mut BaseSnapshotBackend, session_id: &str, sql: &str) {
    match normalize_mutation_sql(catalog, session_id, sql) {
        Operation::Mutation(mutation) => match execute_insert(catalog, backend, &mutation) {
            KernelResult::Mutation(_) => {}
            KernelResult::Refusal(refusal) => {
                panic!(
                    "read corpus seed `{session_id}` refused with SQLSTATE `{}`",
                    refusal.sqlstate
                )
            }
            other => panic!("read corpus seed should yield mutation or refusal, got {other:?}"),
        },
        Operation::Refusal(refusal) => {
            panic!(
                "read corpus seed `{session_id}` refused during normalization with SQLSTATE `{}`",
                normalization_refusal_sqlstate(refusal.code.as_str())
            )
        }
        other => panic!("read corpus seed should normalize to mutation or refusal, got {other:?}"),
    }
}

fn execute_read_sql(
    catalog: &Catalog,
    backend: &BaseSnapshotBackend,
    session_id: &str,
    sql: &str,
) -> TwinReadOutcome {
    match normalize_read_sql(catalog, session_id, sql) {
        Operation::Read(read) => match execute_read(catalog, backend, &read) {
            KernelResult::Read(result) => {
                let columns = result.columns;
                let rows = result
                    .rows
                    .into_iter()
                    .map(|row| differential_row(columns.as_slice(), row))
                    .collect();
                TwinReadOutcome::Success { columns, rows }
            }
            KernelResult::Refusal(refusal) => TwinReadOutcome::Refusal {
                sqlstate: refusal.sqlstate,
            },
            other => panic!("read corpus execution should yield read or refusal, got {other:?}"),
        },
        Operation::Refusal(refusal) => TwinReadOutcome::Refusal {
            sqlstate: normalization_refusal_sqlstate(refusal.code.as_str()).to_owned(),
        },
        other => panic!("read corpus normalization should yield read or refusal, got {other:?}"),
    }
}

fn differential_row(columns: &[String], values: Vec<ScalarValue>) -> DifferentialRow {
    columns
        .iter()
        .cloned()
        .zip(values)
        .map(|(column, value)| (column, scalar_to_json(value)))
        .collect()
}

fn scalar_to_json(value: ScalarValue) -> Value {
    match value {
        ScalarValue::Null => Value::Null,
        ScalarValue::Boolean(value) => Value::Bool(value),
        ScalarValue::Integer(value) => json!(value),
        ScalarValue::Text(value) => Value::String(value),
    }
}

fn normalization_refusal_sqlstate(code: &str) -> &'static str {
    match code {
        "unknown_table" => "42P01",
        "unknown_column" => "42703",
        "duplicate_column" => "42701",
        _ => "0A000",
    }
}

fn row<const N: usize>(entries: [(&str, serde_json::Value); N]) -> DifferentialRow {
    entries
        .into_iter()
        .map(|(column, value)| (column.to_owned(), value))
        .collect()
}
