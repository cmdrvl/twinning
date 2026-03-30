use std::fs;

use serde::Deserialize;
use twinning::{
    backend::BaseSnapshotBackend,
    catalog::{Catalog, parse_postgres_schema},
    ir::{Operation, normalize_mutation_sql},
    kernel::{mutation::execute_insert, storage::TableStorage},
    result::{KernelResult, MutationResult, ResultTag},
};

use super::{
    assertions::{
        OutcomeClass, assert_command_tag, assert_outcome_class, assert_refusal_classification,
        assert_rows_affected, assert_sqlstate,
    },
    runner::{CorpusKind, DifferentialRunner},
};

#[test]
fn write_corpus() {
    let runner = DifferentialRunner::prepare(CorpusKind::Write).expect("prepare differential run");
    let corpus = fs::read_to_string(runner.fixture().corpus_path()).expect("read write corpus");
    let corpus: WriteCorpusFixture =
        serde_json::from_str(&corpus).expect("parse write corpus fixture");
    let schema = fs::read_to_string(runner.fixture().schema_path()).expect("read write schema");
    let catalog = parse_postgres_schema(&schema).expect("parse write schema");

    assert_eq!(runner.fixture().id(), "write_corpus");
    assert!(runner.fixture().fixture_dir().exists());
    assert!(runner.scratch_dir().path().exists());

    for case in &corpus.cases {
        let mut backend = backend_for_catalog(&catalog);
        for (seed_index, seed_sql) in case.seed_sql.iter().enumerate() {
            let seed_outcome = execute_write_sql(
                &catalog,
                &mut backend,
                format!("{}-seed-{}", case.id, seed_index + 1).as_str(),
                seed_sql,
            );
            match seed_outcome {
                TwinWriteOutcome::Success { .. } => {}
                TwinWriteOutcome::Refusal { sqlstate } => {
                    panic!(
                        "write corpus seed `{}` for case `{}` refused with SQLSTATE `{sqlstate}`",
                        seed_index + 1,
                        case.id
                    );
                }
            }
        }

        let outcome = execute_write_sql(&catalog, &mut backend, case.id.as_str(), &case.sql);
        match (case.id.as_str(), outcome) {
            (
                "insert_basic",
                TwinWriteOutcome::Success {
                    command_tag,
                    rows_affected,
                },
            ) => {
                assert_outcome_class(
                    case.expected.outcome_class(),
                    OutcomeClass::Success,
                    "write corpus insert_basic classification",
                );
                assert_command_tag(&command_tag, "INSERT 0 1", "write corpus insert_basic tag");
                assert_rows_affected(rows_affected, 1, "write corpus insert_basic rows");
            }
            (
                "upsert_primary_key",
                TwinWriteOutcome::Success {
                    command_tag,
                    rows_affected,
                },
            ) => {
                assert_outcome_class(
                    case.expected.outcome_class(),
                    OutcomeClass::Success,
                    "write corpus upsert_primary_key classification",
                );
                assert_command_tag(
                    &command_tag,
                    "INSERT 0 1",
                    "write corpus upsert_primary_key tag",
                );
                assert_rows_affected(rows_affected, 1, "write corpus upsert_primary_key rows");
            }
            ("unique_violation", TwinWriteOutcome::Refusal { sqlstate }) => {
                assert_refusal_classification(
                    case.expected.outcome_class(),
                    "write corpus unique_violation classification",
                );
                assert_sqlstate(
                    Some(sqlstate.as_str()),
                    "23505",
                    "write corpus unique_violation sqlstate",
                );
            }
            ("foreign_key_violation", TwinWriteOutcome::Refusal { sqlstate }) => {
                assert_refusal_classification(
                    case.expected.outcome_class(),
                    "write corpus foreign_key_violation classification",
                );
                assert_sqlstate(
                    Some(sqlstate.as_str()),
                    "23503",
                    "write corpus foreign_key_violation sqlstate",
                );
            }
            ("unsupported_insert_select", TwinWriteOutcome::Refusal { sqlstate }) => {
                assert_refusal_classification(
                    case.expected.outcome_class(),
                    "write corpus unsupported_insert_select classification",
                );
                assert_sqlstate(
                    Some(sqlstate.as_str()),
                    "0A000",
                    "write corpus unsupported_insert_select sqlstate",
                );
            }
            ("insert_basic", TwinWriteOutcome::Refusal { sqlstate })
            | ("upsert_primary_key", TwinWriteOutcome::Refusal { sqlstate }) => {
                panic!(
                    "write corpus case `{}` unexpectedly refused with SQLSTATE `{sqlstate}`",
                    case.id
                );
            }
            (
                "unique_violation" | "foreign_key_violation" | "unsupported_insert_select",
                TwinWriteOutcome::Success {
                    command_tag,
                    rows_affected,
                },
            ) => {
                panic!(
                    "write corpus case `{}` unexpectedly succeeded with tag `{command_tag}` and rows `{rows_affected}`",
                    case.id
                );
            }
            (other, _) => panic!("unexpected write corpus case `{other}`"),
        }
    }
}

#[test]
fn write_corpus_fixture_is_checked_in_and_wired() {
    let runner = DifferentialRunner::prepare(CorpusKind::Write).expect("prepare differential run");
    let corpus = fs::read_to_string(runner.fixture().corpus_path()).expect("read write corpus");
    let corpus: WriteCorpusFixture =
        serde_json::from_str(&corpus).expect("parse write corpus fixture");

    assert_eq!(runner.fixture().id(), "write_corpus");
    assert!(runner.fixture().fixture_dir().exists());
    assert!(runner.fixture().schema_path().exists());
    assert!(runner.fixture().corpus_path().exists());
    assert_eq!(corpus.version, "twinning.differential.write-corpus.v0");
    assert_eq!(corpus.cases.len(), 5);

    let case_ids = corpus
        .cases
        .iter()
        .map(|case| case.id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        case_ids,
        vec![
            "insert_basic",
            "upsert_primary_key",
            "unique_violation",
            "foreign_key_violation",
            "unsupported_insert_select",
        ]
    );

    for case in &corpus.cases {
        assert!(
            !case.sql.trim().is_empty(),
            "case `{}` must declare SQL",
            case.id
        );
        assert!(
            !case.seed_sql.is_empty() || case.id == "foreign_key_violation",
            "case `{}` should declare seed SQL unless the scenario depends on missing parents",
            case.id
        );

        match case.id.as_str() {
            "insert_basic" => {
                assert_outcome_class(
                    case.expected.outcome_class(),
                    OutcomeClass::Success,
                    "write corpus insert_basic classification",
                );
                assert_command_tag(
                    case.expected.command_tag.as_deref().unwrap_or("<missing>"),
                    "INSERT 0 1",
                    "write corpus insert_basic tag",
                );
                assert_rows_affected(
                    case.expected.rows_affected.expect("rows affected"),
                    1,
                    "write corpus insert_basic rows",
                );
            }
            "upsert_primary_key" => {
                assert_outcome_class(
                    case.expected.outcome_class(),
                    OutcomeClass::Success,
                    "write corpus upsert_primary_key classification",
                );
                assert_command_tag(
                    case.expected.command_tag.as_deref().unwrap_or("<missing>"),
                    "INSERT 0 1",
                    "write corpus upsert_primary_key tag",
                );
                assert_rows_affected(
                    case.expected.rows_affected.expect("rows affected"),
                    1,
                    "write corpus upsert_primary_key rows",
                );
            }
            "unique_violation" => {
                assert_refusal_classification(
                    case.expected.outcome_class(),
                    "write corpus unique_violation classification",
                );
                assert_sqlstate(
                    case.expected.sqlstate.as_deref(),
                    "23505",
                    "write corpus unique_violation sqlstate",
                );
            }
            "foreign_key_violation" => {
                assert_refusal_classification(
                    case.expected.outcome_class(),
                    "write corpus foreign_key_violation classification",
                );
                assert_sqlstate(
                    case.expected.sqlstate.as_deref(),
                    "23503",
                    "write corpus foreign_key_violation sqlstate",
                );
            }
            "unsupported_insert_select" => {
                assert_refusal_classification(
                    case.expected.outcome_class(),
                    "write corpus unsupported_insert_select classification",
                );
                assert_sqlstate(
                    case.expected.sqlstate.as_deref(),
                    "0A000",
                    "write corpus unsupported_insert_select sqlstate",
                );
            }
            other => panic!("unexpected write corpus case `{other}`"),
        }
    }
}

#[derive(Debug, Deserialize)]
struct WriteCorpusFixture {
    version: String,
    cases: Vec<WriteCorpusCase>,
}

#[derive(Debug, Deserialize)]
struct WriteCorpusCase {
    id: String,
    #[allow(dead_code)]
    description: String,
    seed_sql: Vec<String>,
    sql: String,
    expected: WriteCorpusExpectation,
}

#[derive(Debug, Deserialize)]
struct WriteCorpusExpectation {
    outcome_class: String,
    command_tag: Option<String>,
    rows_affected: Option<u64>,
    sqlstate: Option<String>,
}

impl WriteCorpusExpectation {
    fn outcome_class(&self) -> OutcomeClass {
        match self.outcome_class.as_str() {
            "success" => OutcomeClass::Success,
            "refusal" => OutcomeClass::Refusal,
            "skip" => OutcomeClass::Skip,
            other => panic!("unexpected differential outcome class `{other}`"),
        }
    }
}

enum TwinWriteOutcome {
    Success {
        command_tag: String,
        rows_affected: u64,
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

fn execute_write_sql(
    catalog: &Catalog,
    backend: &mut BaseSnapshotBackend,
    session_id: &str,
    sql: &str,
) -> TwinWriteOutcome {
    match normalize_mutation_sql(catalog, session_id, sql) {
        Operation::Mutation(mutation) => match execute_insert(catalog, backend, &mutation) {
            KernelResult::Mutation(result) => TwinWriteOutcome::Success {
                command_tag: mutation_command_tag(&result),
                rows_affected: result.rows_affected,
            },
            KernelResult::Refusal(refusal) => TwinWriteOutcome::Refusal {
                sqlstate: refusal.sqlstate,
            },
            other => {
                panic!("write corpus execution should yield mutation or refusal, got {other:?}")
            }
        },
        Operation::Refusal(refusal) => TwinWriteOutcome::Refusal {
            sqlstate: normalization_refusal_sqlstate(refusal.code.as_str()).to_owned(),
        },
        other => {
            panic!("write corpus normalization should yield mutation or refusal, got {other:?}")
        }
    }
}

fn mutation_command_tag(result: &MutationResult) -> String {
    match result.tag {
        ResultTag::Insert | ResultTag::Upsert => format!("INSERT 0 {}", result.rows_affected),
        ResultTag::Update => format!("UPDATE {}", result.rows_affected),
        ResultTag::Delete => format!("DELETE {}", result.rows_affected),
        other => panic!("unexpected mutation result tag `{other:?}`"),
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
