use std::path::Path;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{
    backend::BaseSnapshotBackend,
    catalog::Catalog,
    cli::{Engine, ProofArgs, ProofCommand, TwinPairProofArgs},
    declaration::CatalogDeclarationIdentity,
    ir::{
        AggregateSpec, PredicateComparison, PredicateExpr, PredicateOperator, ReadOp, ReadShape,
        ScalarValue,
    },
    kernel::read::execute_read,
    refusal::{self, RefusalEnvelope, RefusalResult},
    result::KernelResult,
    runtime::Execution,
    snapshot::{self, TwinSnapshot, restore},
};

pub const TWIN_PAIR_PROOF_VERSION: &str = "twinning.twin-pair-proof.v0";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TwinPairProofReport {
    pub version: String,
    pub proof_id: String,
    pub outcome: TwinPairProofOutcome,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog_declaration: Option<CatalogDeclarationIdentity>,
    pub endpoints: Vec<TwinPairEndpointIdentity>,
    pub cases: Vec<TwinPairProofCase>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TwinPairProofOutcome {
    Pass,
    Fail,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TwinPairEndpointIdentity {
    pub endpoint_id: String,
    pub role: String,
    pub engine: Engine,
    pub snapshot_hash: String,
    pub committed_state_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog_declaration_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TwinPairProofCase {
    pub case_id: String,
    pub verdict: TwinPairCaseVerdict,
    pub left: TwinPairObservation,
    pub right: TwinPairObservation,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mismatches: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TwinPairCaseVerdict {
    Pass,
    Fail,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TwinPairObservation {
    pub endpoint_id: String,
    pub query_id: String,
    pub result_hash: String,
    pub result: Value,
}

impl TwinPairProofReport {
    pub fn new(
        proof_id: impl Into<String>,
        catalog_declaration: Option<CatalogDeclarationIdentity>,
        endpoints: Vec<TwinPairEndpointIdentity>,
        cases: Vec<TwinPairProofCase>,
    ) -> Self {
        let outcome = if cases
            .iter()
            .all(|case| case.verdict == TwinPairCaseVerdict::Pass)
        {
            TwinPairProofOutcome::Pass
        } else {
            TwinPairProofOutcome::Fail
        };

        Self {
            version: TWIN_PAIR_PROOF_VERSION.to_owned(),
            proof_id: proof_id.into(),
            outcome,
            catalog_declaration,
            endpoints,
            cases,
        }
    }
}

impl TwinPairProofCase {
    pub fn compare(
        case_id: impl Into<String>,
        left: TwinPairObservation,
        right: TwinPairObservation,
    ) -> Self {
        let mismatches = if left.result_hash == right.result_hash && left.result == right.result {
            Vec::new()
        } else {
            vec![String::from("query_result")]
        };
        let verdict = if mismatches.is_empty() {
            TwinPairCaseVerdict::Pass
        } else {
            TwinPairCaseVerdict::Fail
        };

        Self {
            case_id: case_id.into(),
            verdict,
            left,
            right,
            mismatches,
        }
    }
}

pub fn execute(args: &ProofArgs, json_mode: bool) -> Result<Execution, Box<dyn std::error::Error>> {
    let execution = match execute_inner(args, json_mode) {
        Ok(execution) => execution,
        Err(refusal) => Execution {
            exit_code: 2,
            stdout: refusal.render(json_mode)?,
        },
    };

    Ok(execution)
}

fn execute_inner(args: &ProofArgs, json_mode: bool) -> RefusalResult<Execution> {
    match &args.command {
        ProofCommand::TwinPair(args) => execute_twin_pair(args, json_mode),
    }
}

fn execute_twin_pair(args: &TwinPairProofArgs, json_mode: bool) -> RefusalResult<Execution> {
    let query_file = read_query_file(&args.queries)?;
    let left_snapshot = snapshot::read_snapshot(&args.left)?;
    let right_snapshot = snapshot::read_snapshot(&args.right)?;
    validate_compatible_snapshots(&left_snapshot, &right_snapshot)?;

    let left_backend = restore::restore_base_backend(&left_snapshot)?;
    let right_backend = restore::restore_base_backend(&right_snapshot)?;
    let left_endpoint = endpoint_identity("left", "left-snapshot", &left_snapshot)?;
    let right_endpoint = endpoint_identity("right", "right-snapshot", &right_snapshot)?;

    let cases = query_file
        .queries
        .iter()
        .map(|query| {
            TwinPairProofCase::compare(
                query.id.clone(),
                observe_query(
                    &left_endpoint.endpoint_id,
                    &left_snapshot.catalog,
                    &left_backend,
                    query,
                ),
                observe_query(
                    &right_endpoint.endpoint_id,
                    &right_snapshot.catalog,
                    &right_backend,
                    query,
                ),
            )
        })
        .collect::<Vec<_>>();

    let report = TwinPairProofReport::new(
        query_file.proof_id.unwrap_or_else(|| {
            format!(
                "{}:{}",
                TWIN_PAIR_PROOF_VERSION,
                args.queries
                    .file_stem()
                    .and_then(|name| name.to_str())
                    .unwrap_or("snapshot-pair")
            )
        }),
        left_snapshot.catalog_declaration.clone(),
        vec![left_endpoint, right_endpoint],
        cases,
    );

    if let Some(report_path) = &args.report {
        write_report(report_path, &report)?;
    }

    let stdout = if json_mode {
        render_json(&report)?
    } else {
        render_human(&report)
    };

    Ok(Execution {
        exit_code: 0,
        stdout,
    })
}

fn validate_compatible_snapshots(left: &TwinSnapshot, right: &TwinSnapshot) -> RefusalResult<()> {
    if left.engine != Engine::Postgres || right.engine != Engine::Postgres {
        return Err(Box::new(proof_refusal(
            "Twin-pair proof currently supports Postgres snapshots only.",
            json!({
                "left_engine": left.engine,
                "right_engine": right.engine,
            }),
        )));
    }

    if left.schema_hash != right.schema_hash {
        return Err(Box::new(proof_refusal(
            "Twin-pair proof snapshots must share the same schema hash.",
            json!({
                "left_schema_hash": left.schema_hash,
                "right_schema_hash": right.schema_hash,
            }),
        )));
    }

    if left.catalog != right.catalog {
        return Err(Box::new(proof_refusal(
            "Twin-pair proof snapshots must share the same normalized catalog.",
            json!({
                "left_snapshot_hash": left.snapshot_hash,
                "right_snapshot_hash": right.snapshot_hash,
            }),
        )));
    }

    if left.catalog_declaration != right.catalog_declaration {
        return Err(Box::new(proof_refusal(
            "Twin-pair proof snapshots must share the same catalog declaration identity.",
            json!({
                "left_catalog_declaration": left.catalog_declaration,
                "right_catalog_declaration": right.catalog_declaration,
            }),
        )));
    }

    Ok(())
}

fn endpoint_identity(
    endpoint_id: &str,
    role: &str,
    snapshot: &TwinSnapshot,
) -> RefusalResult<TwinPairEndpointIdentity> {
    Ok(TwinPairEndpointIdentity {
        endpoint_id: endpoint_id.to_owned(),
        role: role.to_owned(),
        engine: snapshot.engine,
        snapshot_hash: snapshot.snapshot_hash.clone(),
        committed_state_hash: sha256_prefixed(&snapshot.canonical_committed_state_bytes()?),
        catalog_declaration_hash: snapshot
            .catalog_declaration
            .as_ref()
            .map(|declaration| declaration.hash.clone()),
    })
}

fn observe_query(
    endpoint_id: &str,
    catalog: &Catalog,
    backend: &BaseSnapshotBackend,
    query: &TwinPairProofQuery,
) -> TwinPairObservation {
    let result = match query.read_op() {
        Ok(read_op) => match execute_read(catalog, backend, &read_op) {
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
        },
        Err(message) => json!({
            "outcome_class": "refusal",
            "code": "unsupported_query_shape",
            "sqlstate": "0A000",
            "detail": {
                "message": message,
            },
        }),
    };

    TwinPairObservation {
        endpoint_id: endpoint_id.to_owned(),
        query_id: query.id.clone(),
        result_hash: sha256_json(&result),
        result,
    }
}

fn read_query_file(path: &Path) -> RefusalResult<TwinPairProofQueryFile> {
    let raw = std::fs::read(path).map_err(|error| Box::new(refusal::io_read(path, &error)))?;
    let query_file: TwinPairProofQueryFile = serde_json::from_slice(&raw)
        .map_err(|error| Box::new(proof_parse(path, error.to_string())))?;

    if let Some(proof_version) = &query_file.proof_version
        && proof_version != TWIN_PAIR_PROOF_VERSION
    {
        return Err(Box::new(proof_parse(
            path,
            format!(
                "unsupported proof_version `{proof_version}` (expected `{TWIN_PAIR_PROOF_VERSION}`)"
            ),
        )));
    }

    if query_file.queries.is_empty() {
        return Err(Box::new(proof_parse(
            path,
            "twin-pair proof query file must contain at least one query",
        )));
    }

    Ok(query_file)
}

fn write_report(path: &Path, report: &TwinPairProofReport) -> RefusalResult<()> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .map_err(|error| Box::new(refusal::io_write(path, &error)))?;
    }

    let rendered = render_json(report)?;
    std::fs::write(path, rendered).map_err(|error| Box::new(refusal::io_write(path, &error)))
}

fn render_json(report: &TwinPairProofReport) -> RefusalResult<String> {
    let mut rendered = serde_json::to_string_pretty(report)
        .map_err(|error| Box::new(refusal::serialization(error.to_string())))?;
    rendered.push('\n');
    Ok(rendered)
}

fn render_human(report: &TwinPairProofReport) -> String {
    let mut rendered = format!(
        "TWIN_PAIR_PROOF {:?}\nproof_id: {}\n",
        report.outcome, report.proof_id
    );
    for case in &report.cases {
        rendered.push_str(&format!("{}: {:?}\n", case.case_id, case.verdict));
    }
    rendered
}

fn proof_refusal(message: impl Into<String>, detail: Value) -> RefusalEnvelope {
    RefusalEnvelope::new("E_TWIN_PAIR_PROOF", message, detail, None)
}

fn proof_parse(path: &Path, message: impl Into<String>) -> RefusalEnvelope {
    proof_refusal(
        format!(
            "Twin-pair proof query file failed for `{}`.",
            path.display()
        ),
        json!({ "path": path.display().to_string(), "error": message.into() }),
    )
}

fn sha256_json(value: &Value) -> String {
    let bytes = serde_json::to_vec(value).expect("proof observation serialization should succeed");
    sha256_prefixed(&bytes)
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("sha256:{:x}", digest.finalize())
}

#[derive(Debug, Clone, Deserialize)]
struct TwinPairProofQueryFile {
    #[serde(default)]
    proof_id: Option<String>,
    #[serde(default)]
    proof_version: Option<String>,
    queries: Vec<TwinPairProofQuery>,
}

#[derive(Debug, Clone, Deserialize)]
struct TwinPairProofQuery {
    id: String,
    table: String,
    shape: String,
    projection: Vec<String>,
    lookup_column: String,
    lookup_value: ScalarValue,
}

impl TwinPairProofQuery {
    fn read_op(&self) -> Result<ReadOp, String> {
        if self.shape != "point_lookup" {
            return Err(format!(
                "query `{}` uses unsupported shape `{}` (supported: point_lookup)",
                self.id, self.shape
            ));
        }

        Ok(ReadOp {
            session_id: String::from("twin-pair-proof"),
            table: self.table.clone(),
            shape: ReadShape::PointLookup,
            projection: self.projection.clone(),
            predicate: Some(PredicateExpr::Comparison(PredicateComparison {
                column: self.lookup_column.clone(),
                operator: PredicateOperator::Eq,
                values: vec![self.lookup_value.clone()],
            })),
            aggregate: AggregateSpec::default(),
            group_by: Vec::new(),
            limit: None,
        })
    }
}
