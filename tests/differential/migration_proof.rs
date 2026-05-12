#![forbid(unsafe_code)]

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use twinning::{
    backend::BaseSnapshotBackend,
    catalog::{Catalog, parse_postgres_schema},
    cli::Engine,
    declaration::{CatalogDeclarationIdentity, load_catalog_declaration},
    ir::{
        AggregateKind, AggregateSpec, PredicateComparison, PredicateExpr, PredicateOperator,
        ReadOp, ReadShape, ScalarValue,
    },
    kernel::read::execute_read,
    migration_proof::{
        TWIN_PAIR_PROOF_VERSION, TwinPairCaseVerdict, TwinPairEndpointIdentity,
        TwinPairObservation, TwinPairProofCase, TwinPairProofOutcome, TwinPairProofReport,
    },
    result::KernelResult,
    snapshot::{SnapshotRelations, TwinSnapshot, restore},
};

const FIXTURE_VERSION: &str = "twinning.twin-pair-proof-fixture.v0";
const FIXTURE_ROOT: &str = "tests/fixtures/differential/twin_pair_migration_proof";

#[test]
fn twin_pair_migration_proof_fixture_pins_contract_dependencies() {
    let fixture = load_fixture();

    assert_eq!(fixture.version, FIXTURE_VERSION);
    assert_eq!(fixture.proof_version, TWIN_PAIR_PROOF_VERSION);
    assert!(schema_path().exists());
    assert!(declaration_path().exists());
    assert!(proof_schema_path().exists());

    let dependencies = fixture
        .dependencies
        .iter()
        .map(|dependency| dependency.bead.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(dependencies, BTreeSet::from(["bd-2bh", "bd-28t", "bd-jjz"]));
    for dependency in &fixture.dependencies {
        assert!(
            !dependency.contract.trim().is_empty(),
            "dependency `{}` should name its proof contract",
            dependency.bead
        );
    }

    let coverage_by_shape = fixture
        .coverage_matrix
        .iter()
        .map(|entry| (entry.shape.as_str(), entry))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(coverage_by_shape["point_lookup"].policy, "pass");
    assert!(
        coverage_by_shape["point_lookup"]
            .cases
            .iter()
            .any(|case| case == "byte_identical_snapshot_pass")
    );
    assert!(
        coverage_by_shape["point_lookup"]
            .cases
            .iter()
            .any(|case| case == "intentional_deal_name_divergence")
    );
    assert_eq!(coverage_by_shape["filtered_scan"].policy, "pass");
    assert_eq!(
        coverage_by_shape["filtered_scan"].cases,
        vec![String::from("filtered_scan_parity")]
    );
    assert_eq!(coverage_by_shape["aggregate_count"].policy, "pass");
    assert_eq!(
        coverage_by_shape["aggregate_count"].cases,
        vec![String::from("aggregate_count_parity")]
    );
    assert_eq!(coverage_by_shape["join_or_introspection"].policy, "skip");
    assert!(coverage_by_shape["join_or_introspection"].cases.is_empty());
    assert!(
        !coverage_by_shape["join_or_introspection"]
            .reason
            .as_deref()
            .expect("skip reason")
            .is_empty()
    );

    let query_ids = fixture
        .queries
        .iter()
        .map(|query| query.id.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        query_ids,
        BTreeSet::from([
            "deal_lookup",
            "outside_subset_lookup",
            "tenant_b_filtered_scan",
            "tenant_b_deal_count",
        ])
    );

    let expected_verdicts = fixture
        .cases
        .iter()
        .map(|case| case.expected_verdict)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        expected_verdicts,
        BTreeSet::from([TwinPairCaseVerdict::Pass, TwinPairCaseVerdict::Fail])
    );
}

#[test]
fn twin_pair_migration_proof_reports_pass_and_intentional_divergence() {
    let fixture = load_fixture();
    let pass_case = fixture.case("byte_identical_snapshot_pass");
    let fail_case = fixture.case("intentional_deal_name_divergence");
    let refusal_case = fixture.case("outside_subset_sqlstate_parity");
    let filtered_case = fixture.case("filtered_scan_parity");
    let aggregate_case = fixture.case("aggregate_count_parity");

    let pass_report = build_report(&fixture, pass_case);
    assert_eq!(pass_report.version, TWIN_PAIR_PROOF_VERSION);
    assert_eq!(pass_report.outcome, TwinPairProofOutcome::Pass);
    assert_eq!(pass_report.endpoints.len(), 2);
    assert_eq!(pass_report.cases[0].verdict, TwinPairCaseVerdict::Pass);
    assert!(pass_report.cases[0].mismatches.is_empty());
    assert_eq!(
        pass_report.endpoints[0].committed_state_hash,
        pass_report.endpoints[1].committed_state_hash
    );
    assert_eq!(
        pass_report.catalog_declaration.as_ref().map(|declaration| {
            (
                declaration.declaration_id.as_str(),
                declaration.catalog_version.as_str(),
            )
        }),
        Some((
            "metadata-catalog:migration-proof:v0",
            "metadata-catalog.v2026-05-12",
        ))
    );

    let fail_report = build_report(&fixture, fail_case);
    assert_eq!(fail_report.outcome, TwinPairProofOutcome::Fail);
    assert_eq!(fail_report.cases[0].verdict, TwinPairCaseVerdict::Fail);
    assert_eq!(fail_report.cases[0].mismatches, vec!["query_result"]);
    assert_ne!(
        fail_report.endpoints[0].committed_state_hash,
        fail_report.endpoints[1].committed_state_hash
    );
    assert_ne!(
        fail_report.cases[0].left.result,
        fail_report.cases[0].right.result
    );

    let refusal_report = build_report(&fixture, refusal_case);
    assert_eq!(refusal_report.outcome, TwinPairProofOutcome::Pass);
    assert_eq!(refusal_report.cases[0].verdict, TwinPairCaseVerdict::Pass);
    assert_eq!(
        refusal_report.cases[0].left.result["sqlstate"],
        json!("42P01")
    );
    assert_eq!(
        refusal_report.cases[0].right.result["sqlstate"],
        json!("42P01")
    );
    assert_eq!(
        refusal_report.cases[0].left.result["code"],
        json!("unknown_table")
    );

    let filtered_report = build_report(&fixture, filtered_case);
    assert_eq!(filtered_report.outcome, TwinPairProofOutcome::Pass);
    assert_eq!(
        filtered_report.cases[0].left.result["rows"][0][0],
        json!({ "text": "deal-002" })
    );

    let aggregate_report = build_report(&fixture, aggregate_case);
    assert_eq!(aggregate_report.outcome, TwinPairProofOutcome::Pass);
    assert_eq!(
        aggregate_report.cases[0].left.result["columns"],
        json!(["deal_count"])
    );
    assert_eq!(
        aggregate_report.cases[0].left.result["rows"][0][0],
        json!({ "integer": 1 })
    );

    let workspace = tempdir().expect("proof report workspace");
    let pass_path = workspace.path().join("pass-proof.json");
    let fail_path = workspace.path().join("fail-proof.json");
    write_report(&pass_path, &pass_report);
    write_report(&fail_path, &fail_report);

    let rendered_fail: Value =
        serde_json::from_str(&fs::read_to_string(&fail_path).expect("read fail report"))
            .expect("parse fail report");
    assert_eq!(rendered_fail["version"], TWIN_PAIR_PROOF_VERSION);
    assert_eq!(rendered_fail["outcome"], "FAIL");
    assert_eq!(
        rendered_fail["endpoints"]
            .as_array()
            .expect("endpoints")
            .len(),
        2
    );
    assert_eq!(
        rendered_fail["cases"][0]["mismatches"],
        json!(["query_result"])
    );
}

fn build_report(fixture: &ProofFixture, case: &FixtureCase) -> TwinPairProofReport {
    let (catalog, schema_hash, declaration) = load_declared_catalog();
    let query = fixture.query(&case.query_id);
    let left = build_endpoint(&catalog, &schema_hash, &declaration, &case.left);
    let right = build_endpoint(&catalog, &schema_hash, &declaration, &case.right);

    let proof_case = TwinPairProofCase::compare(
        case.id.clone(),
        observe_query(&left.identity.endpoint_id, &catalog, &left.backend, query),
        observe_query(&right.identity.endpoint_id, &catalog, &right.backend, query),
    );
    assert_eq!(proof_case.verdict, case.expected_verdict);
    assert_eq!(proof_case.mismatches, case.expected_mismatches);

    TwinPairProofReport::new(
        format!("{}:{}", fixture.proof_version, case.id),
        Some(declaration),
        vec![left.identity, right.identity],
        vec![proof_case],
    )
}

fn build_endpoint(
    catalog: &Catalog,
    schema_hash: &str,
    declaration: &CatalogDeclarationIdentity,
    endpoint: &FixtureEndpoint,
) -> BuiltEndpoint {
    let relations: SnapshotRelations =
        serde_json::from_str(&fs::read_to_string(fixture_dir().join(&endpoint.relations)).unwrap())
            .expect("parse endpoint relations");
    let snapshot = TwinSnapshot::new(
        Engine::Postgres,
        endpoint.relations.clone(),
        schema_hash.to_owned(),
        None,
        None,
        catalog.clone(),
    )
    .expect("build endpoint snapshot")
    .with_catalog_declaration(Some(declaration.clone()))
    .expect("attach declaration")
    .with_relations(relations)
    .expect("attach relations");
    let committed_state_hash = sha256_prefixed(
        &snapshot
            .canonical_committed_state_bytes()
            .expect("committed-state bytes"),
    );
    let backend = restore::restore_base_backend(&snapshot).expect("restore endpoint backend");

    BuiltEndpoint {
        identity: TwinPairEndpointIdentity {
            endpoint_id: endpoint.endpoint_id.clone(),
            role: endpoint.role.clone(),
            engine: Engine::Postgres,
            snapshot_hash: snapshot.snapshot_hash,
            committed_state_hash,
            catalog_declaration_hash: Some(declaration.hash.clone()),
        },
        backend,
    }
}

fn observe_query(
    endpoint_id: &str,
    catalog: &Catalog,
    backend: &BaseSnapshotBackend,
    query: &ProofQuery,
) -> TwinPairObservation {
    let read_op = query
        .read_op()
        .expect("proof query shape should be declared in the replay fixture subset");
    let result = match execute_read(catalog, backend, &read_op) {
        KernelResult::Read(read) => json!({
            "outcome_class": "success",
            "columns": read.columns,
            "rows": read.rows,
        }),
        KernelResult::Refusal(refusal) => json!({
            "outcome_class": "refusal",
            "code": refusal.code,
            "sqlstate": refusal.sqlstate,
            "detail": refusal.detail,
        }),
        other => json!({
            "outcome_class": "unexpected",
            "debug": format!("{other:?}"),
        }),
    };

    TwinPairObservation {
        endpoint_id: endpoint_id.to_owned(),
        query_id: query.id.clone(),
        result_hash: sha256_json(&result),
        result,
    }
}

fn load_declared_catalog() -> (Catalog, String, CatalogDeclarationIdentity) {
    let schema_bytes = fs::read(schema_path()).expect("read schema");
    let schema_hash = sha256_prefixed(&schema_bytes);
    let catalog = parse_postgres_schema(
        &String::from_utf8(schema_bytes).expect("schema fixture should be utf-8"),
    )
    .expect("parse schema");
    let declaration =
        load_catalog_declaration(&declaration_path(), &schema_hash, &catalog).expect("declaration");

    (catalog, schema_hash, declaration)
}

fn load_fixture() -> ProofFixture {
    serde_json::from_str(&fs::read_to_string(fixture_dir().join("cases.json")).expect("read cases"))
        .expect("parse cases")
}

fn write_report(path: &Path, report: &TwinPairProofReport) {
    fs::write(
        path,
        format!(
            "{}\n",
            serde_json::to_string_pretty(report).expect("render proof report")
        ),
    )
    .expect("write proof report");
}

fn sha256_json(value: &Value) -> String {
    let bytes = serde_json::to_vec(value).expect("serialize proof observation");
    sha256_prefixed(&bytes)
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("sha256:{:x}", digest.finalize())
}

fn fixture_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURE_ROOT)
}

fn schema_path() -> PathBuf {
    fixture_dir().join("schema.sql")
}

fn declaration_path() -> PathBuf {
    fixture_dir().join("declaration.json")
}

fn proof_schema_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("schemas")
        .join("twinning.twin-pair-proof.v0.schema.json")
}

struct BuiltEndpoint {
    identity: TwinPairEndpointIdentity,
    backend: BaseSnapshotBackend,
}

#[derive(Debug, Deserialize)]
struct ProofFixture {
    version: String,
    proof_version: String,
    dependencies: Vec<ProofDependency>,
    coverage_matrix: Vec<CoverageEntry>,
    queries: Vec<ProofQuery>,
    cases: Vec<FixtureCase>,
}

impl ProofFixture {
    fn query(&self, query_id: &str) -> &ProofQuery {
        self.queries
            .iter()
            .find(|query| query.id == query_id)
            .expect("missing query fixture")
    }

    fn case(&self, case_id: &str) -> &FixtureCase {
        self.cases
            .iter()
            .find(|case| case.id == case_id)
            .expect("missing proof case")
    }
}

#[derive(Debug, Deserialize)]
struct ProofDependency {
    bead: String,
    contract: String,
}

#[derive(Debug, Deserialize)]
struct CoverageEntry {
    shape: String,
    policy: String,
    #[serde(default)]
    cases: Vec<String>,
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ProofQuery {
    id: String,
    table: String,
    shape: String,
    projection: Vec<String>,
    lookup_column: String,
    lookup_value: ScalarValue,
    #[serde(default)]
    count_column: Option<String>,
    #[serde(default)]
    aggregate_alias: Option<String>,
    #[serde(default)]
    limit: Option<u64>,
}

impl ProofQuery {
    fn read_op(&self) -> Result<ReadOp, String> {
        let predicate = Some(PredicateExpr::Comparison(PredicateComparison {
            column: self.lookup_column.clone(),
            operator: PredicateOperator::Eq,
            values: vec![self.lookup_value.clone()],
        }));

        match self.shape.as_str() {
            "point_lookup" => Ok(ReadOp {
                session_id: String::from("migration-proof"),
                table: self.table.clone(),
                shape: ReadShape::PointLookup,
                projection: self.projection.clone(),
                predicate,
                aggregate: AggregateSpec::default(),
                group_by: Vec::new(),
                limit: None,
            }),
            "filtered_scan" => Ok(ReadOp {
                session_id: String::from("migration-proof"),
                table: self.table.clone(),
                shape: ReadShape::FilteredScan,
                projection: self.projection.clone(),
                predicate,
                aggregate: AggregateSpec::default(),
                group_by: Vec::new(),
                limit: self.limit,
            }),
            "aggregate_count" => Ok(ReadOp {
                session_id: String::from("migration-proof"),
                table: self.table.clone(),
                shape: ReadShape::AggregateScan,
                projection: Vec::new(),
                predicate,
                aggregate: AggregateSpec {
                    kind: AggregateKind::Count,
                    column: self.count_column.clone(),
                    alias: self.aggregate_alias.clone(),
                },
                group_by: Vec::new(),
                limit: None,
            }),
            other => Err(format!(
                "query `{}` uses unsupported fixture shape `{}`",
                self.id, other
            )),
        }
    }
}

#[derive(Debug, Deserialize)]
struct FixtureCase {
    id: String,
    query_id: String,
    expected_verdict: TwinPairCaseVerdict,
    left: FixtureEndpoint,
    right: FixtureEndpoint,
    expected_mismatches: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct FixtureEndpoint {
    endpoint_id: String,
    role: String,
    relations: String,
}
