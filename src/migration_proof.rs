use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlparser::{dialect::PostgreSqlDialect, parser::Parser};

use crate::{
    backend::{Backend, BaseSnapshotBackend},
    catalog::{Catalog, parse_postgres_schema},
    cli::{
        Engine, ProofArgs, ProofCommand, TwinPairOrchestrateArgs, TwinPairProofArgs,
        TwinPairProofCommand,
    },
    declaration::{CatalogDeclarationIdentity, load_catalog_declaration},
    ir::{
        AggregateKind, AggregateSpec, Operation, PredicateComparison, PredicateExpr,
        PredicateOperator, ReadOp, ReadShape, ScalarValue, normalize_mutation_statement,
    },
    kernel::{mutation::execute_mutation, read::execute_read, storage::TableStorage},
    orchestration_manifest::{
        ManifestArtifactRef, TwinPairEndpointBootstrap, TwinPairEndpointSpec,
        TwinPairOrchestrationManifest, load_twin_pair_orchestration_manifest,
    },
    refusal::{self, RefusalEnvelope, RefusalResult},
    report::SourceMaterializationReport,
    result::KernelResult,
    runtime::Execution,
    snapshot::{self, TwinSnapshot, restore},
};

pub const TWIN_PAIR_PROOF_VERSION: &str = "twinning.twin-pair-proof.v0";
pub const TWIN_PAIR_REPLAY_MANIFEST_VERSION: &str = "twinning.twin-pair-replay-manifest.v0";
pub const TWIN_PAIR_REPLAY_RESULT_VERSION: &str = "twinning.twin-pair-replay-result.v0";

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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_materialization: Option<SourceMaterializationReport>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub evidence_identities: Vec<TwinPairEvidenceIdentity>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TwinPairEvidenceIdentity {
    pub artifact_kind: TwinPairEvidenceKind,
    pub artifact_id: String,
    pub version: String,
    pub hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TwinPairEvidenceKind {
    Verify,
    Benchmark,
    Assess,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TwinPairProofCase {
    pub case_id: String,
    pub verdict: TwinPairCaseVerdict,
    pub left: TwinPairObservation,
    pub right: TwinPairObservation,
    pub replay_result: TwinPairReplayResultArtifact,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mismatches: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TwinPairCaseVerdict {
    Pass,
    Fail,
    Skip,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TwinPairObservation {
    pub endpoint_id: String,
    pub snapshot_hash: String,
    pub query_id: String,
    pub result_hash: String,
    pub result: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TwinPairReplayResultArtifact {
    pub version: String,
    pub query_id: String,
    pub left_endpoint_id: String,
    pub right_endpoint_id: String,
    pub left_snapshot_hash: String,
    pub right_snapshot_hash: String,
    pub left_result_hash: String,
    pub right_result_hash: String,
    pub result_hashes_match: bool,
    pub row_count_parity: TwinPairOptionalU64Parity,
    pub command_tag_parity: TwinPairOptionalStringParity,
    pub sqlstate_parity: TwinPairSqlstateParity,
    pub refusal_parity: TwinPairOptionalStringParity,
    pub ordering_policy_parity: TwinPairOptionalStringParity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skip_reason_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TwinPairSqlstateParity {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub left: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub right: Option<String>,
    pub matches: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TwinPairOptionalStringParity {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub left: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub right: Option<String>,
    pub matches: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TwinPairOptionalU64Parity {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub left: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub right: Option<u64>,
    pub matches: bool,
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
            .any(|case| case.verdict == TwinPairCaseVerdict::Fail)
        {
            TwinPairProofOutcome::Fail
        } else {
            TwinPairProofOutcome::Pass
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
        let replay_result = TwinPairReplayResultArtifact::from_observations(&left, &right);
        let mut mismatches = Vec::new();
        if !replay_result.result_hashes_match {
            mismatches.push(String::from("result_hash"));
        }
        if !replay_result.row_count_parity.matches {
            mismatches.push(String::from("row_count"));
        }
        if !replay_result.command_tag_parity.matches {
            mismatches.push(String::from("command_tag"));
        }
        if !replay_result.sqlstate_parity.matches {
            mismatches.push(String::from("sqlstate"));
        }
        if !replay_result.refusal_parity.matches {
            mismatches.push(String::from("refusal_envelope"));
        }
        if !replay_result.ordering_policy_parity.matches {
            mismatches.push(String::from("ordering_policy"));
        }

        let verdict = if is_skip_observation(&left) && is_skip_observation(&right) {
            if mismatches.is_empty() {
                TwinPairCaseVerdict::Skip
            } else {
                TwinPairCaseVerdict::Fail
            }
        } else if mismatches.is_empty() {
            TwinPairCaseVerdict::Pass
        } else {
            TwinPairCaseVerdict::Fail
        };

        Self {
            case_id: case_id.into(),
            verdict,
            left,
            right,
            replay_result,
            mismatches,
        }
    }
}

impl TwinPairReplayResultArtifact {
    fn from_observations(left: &TwinPairObservation, right: &TwinPairObservation) -> Self {
        let left_sqlstate = observation_sqlstate(&left.result);
        let right_sqlstate = observation_sqlstate(&right.result);
        let sqlstate_matches = left_sqlstate == right_sqlstate;
        let left_row_count = observation_u64(&left.result, "row_count");
        let right_row_count = observation_u64(&right.result, "row_count");
        let left_command_tag = observation_string(&left.result, "command_tag");
        let right_command_tag = observation_string(&right.result, "command_tag");
        let left_refusal_hash = observation_string(&left.result, "refusal_hash");
        let right_refusal_hash = observation_string(&right.result, "refusal_hash");
        let left_ordering_policy = observation_string(&left.result, "ordering_policy");
        let right_ordering_policy = observation_string(&right.result, "ordering_policy");

        Self {
            version: TWIN_PAIR_REPLAY_RESULT_VERSION.to_owned(),
            query_id: left.query_id.clone(),
            left_endpoint_id: left.endpoint_id.clone(),
            right_endpoint_id: right.endpoint_id.clone(),
            left_snapshot_hash: left.snapshot_hash.clone(),
            right_snapshot_hash: right.snapshot_hash.clone(),
            left_result_hash: left.result_hash.clone(),
            right_result_hash: right.result_hash.clone(),
            result_hashes_match: left.result_hash == right.result_hash,
            row_count_parity: TwinPairOptionalU64Parity {
                left: left_row_count,
                right: right_row_count,
                matches: left_row_count == right_row_count,
            },
            command_tag_parity: TwinPairOptionalStringParity {
                left: left_command_tag.clone(),
                right: right_command_tag.clone(),
                matches: left_command_tag == right_command_tag,
            },
            sqlstate_parity: TwinPairSqlstateParity {
                left: left_sqlstate,
                right: right_sqlstate,
                matches: sqlstate_matches,
            },
            refusal_parity: TwinPairOptionalStringParity {
                left: left_refusal_hash.clone(),
                right: right_refusal_hash.clone(),
                matches: left_refusal_hash == right_refusal_hash,
            },
            ordering_policy_parity: TwinPairOptionalStringParity {
                left: left_ordering_policy.clone(),
                right: right_ordering_policy.clone(),
                matches: left_ordering_policy == right_ordering_policy,
            },
            skip_reason_code: observation_string(&left.result, "reason_code").filter(
                |left_reason| {
                    Some(left_reason.clone()) == observation_string(&right.result, "reason_code")
                },
            ),
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
    match &args.command {
        Some(TwinPairProofCommand::Orchestrate(args)) => {
            execute_twin_pair_orchestrate(args, json_mode)
        }
        None => execute_twin_pair_snapshots(args, json_mode),
    }
}

fn execute_twin_pair_snapshots(
    args: &TwinPairProofArgs,
    json_mode: bool,
) -> RefusalResult<Execution> {
    let queries_path = required_snapshot_pair_arg(args.queries.as_deref(), "--queries")?;
    let left_path = required_snapshot_pair_arg(args.left.as_deref(), "--left")?;
    let right_path = required_snapshot_pair_arg(args.right.as_deref(), "--right")?;
    let query_file = read_query_file(queries_path)?;
    let left_snapshot = snapshot::read_snapshot(left_path)?;
    let right_snapshot = snapshot::read_snapshot(right_path)?;
    let replay = build_twin_pair_replay(
        &query_file,
        &left_snapshot,
        &right_snapshot,
        EndpointReportSpec {
            endpoint_id: "left",
            role: "left-snapshot",
            evidence_identities: Vec::new(),
        },
        EndpointReportSpec {
            endpoint_id: "right",
            role: "right-snapshot",
            evidence_identities: query_file.target_evidence.clone(),
        },
    )?;
    let report = TwinPairProofReport::new(
        query_file.proof_id.unwrap_or_else(|| {
            format!(
                "{}:{}",
                TWIN_PAIR_PROOF_VERSION,
                queries_path
                    .file_stem()
                    .and_then(|name| name.to_str())
                    .unwrap_or("snapshot-pair")
            )
        }),
        left_snapshot.catalog_declaration.clone(),
        replay.endpoints,
        replay.cases,
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

fn execute_twin_pair_orchestrate(
    args: &TwinPairOrchestrateArgs,
    json_mode: bool,
) -> RefusalResult<Execution> {
    let manifest = load_twin_pair_orchestration_manifest(&args.manifest)?;
    validate_orchestration_artifact_hash(
        &args.manifest,
        "replay_manifest",
        &manifest.replay_manifest,
    )?;
    validate_orchestration_artifact_hash(
        &args.manifest,
        "catalog_declaration",
        &manifest.catalog_declaration,
    )?;

    let query_file = read_query_file(&resolve_manifest_path(
        &args.manifest,
        &manifest.replay_manifest.path,
    ))?;
    let left_snapshot =
        orchestration_endpoint_snapshot(&args.manifest, "left_endpoint", &manifest.left_endpoint)?;
    let right_snapshot = orchestration_endpoint_snapshot(
        &args.manifest,
        "right_endpoint",
        &manifest.right_endpoint,
    )?;
    validate_orchestration_declaration(&args.manifest, &manifest, &left_snapshot, &right_snapshot)?;

    let replay = build_twin_pair_replay(
        &query_file,
        &left_snapshot,
        &right_snapshot,
        EndpointReportSpec {
            endpoint_id: &manifest.left_endpoint.endpoint_id,
            role: &manifest.left_endpoint.role,
            evidence_identities: Vec::new(),
        },
        EndpointReportSpec {
            endpoint_id: &manifest.right_endpoint.endpoint_id,
            role: &manifest.right_endpoint.role,
            evidence_identities: manifest.target_evidence.clone(),
        },
    )?;
    let report = TwinPairProofReport::new(
        manifest.proof_id.clone(),
        left_snapshot.catalog_declaration.clone(),
        replay.endpoints,
        replay.cases,
    );

    let bundle_dir = args.bundle_dir.clone().unwrap_or_else(|| {
        resolve_manifest_path(&args.manifest, &manifest.artifact_outputs.bundle_dir)
    });
    fs::create_dir_all(&bundle_dir)
        .map_err(|error| Box::new(refusal::io_write(&bundle_dir, &error)))?;

    let left_output =
        resolve_manifest_path(&args.manifest, &manifest.artifact_outputs.left_snapshot);
    let right_output =
        resolve_manifest_path(&args.manifest, &manifest.artifact_outputs.right_snapshot);
    snapshot::write_snapshot(&left_output, &left_snapshot)?;
    snapshot::write_snapshot(&right_output, &right_snapshot)?;

    let report_path = args.report.clone().unwrap_or_else(|| {
        resolve_manifest_path(&args.manifest, &manifest.artifact_outputs.report)
    });
    write_report(&report_path, &report)?;

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

struct EndpointReportSpec<'a> {
    endpoint_id: &'a str,
    role: &'a str,
    evidence_identities: Vec<TwinPairEvidenceIdentity>,
}

struct BuiltTwinPairReplay {
    endpoints: Vec<TwinPairEndpointIdentity>,
    cases: Vec<TwinPairProofCase>,
}

fn build_twin_pair_replay(
    query_file: &TwinPairProofQueryFile,
    left_snapshot: &TwinSnapshot,
    right_snapshot: &TwinSnapshot,
    left_spec: EndpointReportSpec<'_>,
    right_spec: EndpointReportSpec<'_>,
) -> RefusalResult<BuiltTwinPairReplay> {
    validate_compatible_snapshots(left_snapshot, right_snapshot)?;

    let left_backend = restore::restore_base_backend(left_snapshot)?;
    let right_backend = restore::restore_base_backend(right_snapshot)?;
    let left_endpoint = endpoint_identity(
        left_spec.endpoint_id,
        left_spec.role,
        left_snapshot,
        left_spec.evidence_identities,
    )?;
    let right_endpoint = endpoint_identity(
        right_spec.endpoint_id,
        right_spec.role,
        right_snapshot,
        right_spec.evidence_identities,
    )?;

    let cases = query_file
        .queries
        .iter()
        .map(|query| {
            TwinPairProofCase::compare(
                query.id.clone(),
                observe_query(
                    &left_endpoint.endpoint_id,
                    &left_endpoint.snapshot_hash,
                    &left_snapshot.catalog,
                    &left_backend,
                    query,
                ),
                observe_query(
                    &right_endpoint.endpoint_id,
                    &right_endpoint.snapshot_hash,
                    &right_snapshot.catalog,
                    &right_backend,
                    query,
                ),
            )
        })
        .collect();

    Ok(BuiltTwinPairReplay {
        endpoints: vec![left_endpoint, right_endpoint],
        cases,
    })
}

fn required_snapshot_pair_arg<'a>(path: Option<&'a Path>, flag: &str) -> RefusalResult<&'a Path> {
    path.ok_or_else(|| {
        Box::new(proof_refusal(
            format!("Twin-pair snapshot proof requires `{flag}`."),
            json!({ "missing": flag }),
        ))
    })
}

fn orchestration_endpoint_snapshot(
    manifest_path: &Path,
    field: &str,
    endpoint: &TwinPairEndpointSpec,
) -> RefusalResult<TwinSnapshot> {
    match &endpoint.bootstrap {
        TwinPairEndpointBootstrap::Restore { snapshot } => {
            let snapshot_path = resolve_manifest_path(manifest_path, snapshot);
            snapshot::read_snapshot(&snapshot_path)
        }
        TwinPairEndpointBootstrap::Schema {
            schema,
            declaration,
            load,
        } => {
            let schema_path = resolve_manifest_path(manifest_path, schema);
            let schema_bytes = fs::read(&schema_path)
                .map_err(|error| Box::new(refusal::io_read(&schema_path, &error)))?;
            let schema_hash = sha256_prefixed(&schema_bytes);
            let schema_source = String::from_utf8(schema_bytes).map_err(|error| {
                Box::new(proof_refusal(
                    "Twin-pair orchestration schema file must be UTF-8 Postgres DDL.",
                    json!({
                        "field": field,
                        "schema": schema_path.display().to_string(),
                        "error": error.to_string(),
                    }),
                ))
            })?;
            let catalog = parse_postgres_schema(&schema_source).map_err(|error| {
                Box::new(refusal::schema_parse(&schema_path, error.to_string()))
            })?;
            let declaration = declaration
                .as_ref()
                .map(|declaration| {
                    let declaration_path = resolve_manifest_path(manifest_path, declaration);
                    load_catalog_declaration(&declaration_path, &schema_hash, &catalog)
                })
                .transpose()?;

            let snapshot = TwinSnapshot::new(
                Engine::Postgres,
                schema_path.display().to_string(),
                schema_hash,
                None,
                None,
                catalog,
            )?
            .with_catalog_declaration(declaration)?;

            if load.is_empty() {
                return Ok(snapshot);
            }

            let loaded = materialize_schema_loads(manifest_path, field, endpoint, &snapshot, load)?;
            snapshot
                .with_source_materialization(Some(loaded.report))?
                .with_committed_tables(loaded.committed_tables)
        }
    }
}

struct MaterializedEndpointLoad {
    committed_tables: Vec<TableStorage>,
    report: SourceMaterializationReport,
}

fn materialize_schema_loads(
    manifest_path: &Path,
    field: &str,
    endpoint: &TwinPairEndpointSpec,
    snapshot: &TwinSnapshot,
    load: &[String],
) -> RefusalResult<MaterializedEndpointLoad> {
    let mut backend = BaseSnapshotBackend::new(empty_tables(&snapshot.catalog)?)
        .map_err(|error| Box::new(refusal::serialization(error.to_string())))?;
    let mut source_hasher = Sha256::new();
    let dialect = PostgreSqlDialect {};

    for load_item in load {
        let load_path = resolve_manifest_path(manifest_path, load_item);
        let load_bytes =
            fs::read(&load_path).map_err(|error| Box::new(refusal::io_read(&load_path, &error)))?;
        source_hasher.update(load_item.as_bytes());
        source_hasher.update([0]);
        source_hasher.update(sha256_prefixed(&load_bytes).as_bytes());
        source_hasher.update([0xff]);

        let load_sql = String::from_utf8(load_bytes).map_err(|error| {
            Box::new(proof_refusal(
                "Twin-pair orchestration schema load file must be UTF-8 Postgres SQL.",
                json!({
                    "field": field,
                    "endpoint_id": endpoint.endpoint_id,
                    "load": load_path.display().to_string(),
                    "error": error.to_string(),
                }),
            ))
        })?;
        let statements = Parser::parse_sql(&dialect, &load_sql).map_err(|error| {
            Box::new(proof_refusal(
                "Twin-pair orchestration schema load file failed to parse.",
                json!({
                    "field": field,
                    "endpoint_id": endpoint.endpoint_id,
                    "load": load_path.display().to_string(),
                    "error": error.to_string(),
                }),
            ))
        })?;
        if statements.is_empty() {
            return Err(Box::new(proof_refusal(
                "Twin-pair orchestration schema load file did not contain any statements.",
                json!({
                    "field": field,
                    "endpoint_id": endpoint.endpoint_id,
                    "load": load_path.display().to_string(),
                }),
            )));
        }

        for (statement_index, statement) in statements.iter().enumerate() {
            let mutation = match normalize_mutation_statement(
                &snapshot.catalog,
                format!("{}:load", endpoint.endpoint_id),
                statement,
            ) {
                Operation::Mutation(mutation) => mutation,
                Operation::Refusal(refusal) => {
                    return Err(Box::new(proof_refusal(
                        "Twin-pair orchestration schema load contains an unsupported statement.",
                        json!({
                            "field": field,
                            "endpoint_id": endpoint.endpoint_id,
                            "load": load_path.display().to_string(),
                            "statement_index": statement_index,
                            "statement": statement.to_string(),
                            "refusal": refusal,
                            "policy": "manifest_sql_load_supports_declared_mutations_only",
                        }),
                    )));
                }
                other => {
                    return Err(Box::new(proof_refusal(
                        "Twin-pair orchestration schema load normalized to an unexpected operation.",
                        json!({
                            "field": field,
                            "endpoint_id": endpoint.endpoint_id,
                            "load": load_path.display().to_string(),
                            "statement_index": statement_index,
                            "operation": format!("{other:?}"),
                        }),
                    )));
                }
            };

            match execute_mutation(&snapshot.catalog, &mut backend, &mutation) {
                KernelResult::Mutation(_) => {}
                KernelResult::Refusal(refusal) => {
                    return Err(Box::new(proof_refusal(
                        "Twin-pair orchestration schema load failed kernel mutation execution.",
                        json!({
                            "field": field,
                            "endpoint_id": endpoint.endpoint_id,
                            "load": load_path.display().to_string(),
                            "statement_index": statement_index,
                            "statement": statement.to_string(),
                            "refusal": refusal,
                        }),
                    )));
                }
                other => {
                    return Err(Box::new(proof_refusal(
                        "Twin-pair orchestration schema load produced an unexpected kernel result.",
                        json!({
                            "field": field,
                            "endpoint_id": endpoint.endpoint_id,
                            "load": load_path.display().to_string(),
                            "statement_index": statement_index,
                            "result": format!("{other:?}"),
                        }),
                    )));
                }
            }
        }
    }

    let committed_tables = committed_tables_from_backend(&snapshot.catalog, &backend)?;
    let table_rows = committed_tables
        .iter()
        .map(|table| (table.table_name().to_owned(), table.row_count() as u64))
        .collect::<std::collections::BTreeMap<_, _>>();
    let row_count = table_rows.values().copied().sum();

    Ok(MaterializedEndpointLoad {
        committed_tables,
        report: SourceMaterializationReport {
            source_identity: format!("sha256:{:x}", source_hasher.finalize()),
            method: String::from("manifest_sql_load"),
            table_count: snapshot.catalog.tables.len(),
            row_count,
            tables: table_rows,
        },
    })
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

fn committed_tables_from_backend(
    catalog: &Catalog,
    backend: &BaseSnapshotBackend,
) -> RefusalResult<Vec<TableStorage>> {
    catalog
        .tables
        .iter()
        .map(|table| {
            backend.visible_table(&table.name).cloned().ok_or_else(|| {
                Box::new(refusal::serialization(format!(
                    "schema load backend did not retain table `{}`",
                    table.name
                )))
            })
        })
        .collect()
}

fn validate_orchestration_declaration(
    manifest_path: &Path,
    manifest: &TwinPairOrchestrationManifest,
    left: &TwinSnapshot,
    right: &TwinSnapshot,
) -> RefusalResult<()> {
    let declaration_path = resolve_manifest_path(manifest_path, &manifest.catalog_declaration.path);
    let declaration =
        load_catalog_declaration(&declaration_path, &left.schema_hash, &left.catalog)?;
    if left.catalog_declaration.as_ref() != Some(&declaration)
        || right.catalog_declaration.as_ref() != Some(&declaration)
    {
        return Err(Box::new(proof_refusal(
            "Twin-pair orchestration snapshots must match the manifest catalog declaration.",
            json!({
                "catalog_declaration": declaration_path.display().to_string(),
                "expected_hash": declaration.hash,
                "left_catalog_declaration": &left.catalog_declaration,
                "right_catalog_declaration": &right.catalog_declaration,
            }),
        )));
    }

    Ok(())
}

fn validate_orchestration_artifact_hash(
    manifest_path: &Path,
    field: &str,
    artifact: &ManifestArtifactRef,
) -> RefusalResult<()> {
    let Some(expected_hash) = &artifact.hash else {
        return Ok(());
    };
    let artifact_path = resolve_manifest_path(manifest_path, &artifact.path);
    let bytes = fs::read(&artifact_path)
        .map_err(|error| Box::new(refusal::io_read(&artifact_path, &error)))?;
    let actual_hash = sha256_prefixed(&bytes);
    if actual_hash != *expected_hash {
        return Err(Box::new(proof_refusal(
            "Twin-pair orchestration artifact hash mismatch.",
            json!({
                "field": field,
                "path": artifact_path.display().to_string(),
                "expected_hash": expected_hash,
                "actual_hash": actual_hash,
            }),
        )));
    }

    Ok(())
}

fn resolve_manifest_path(manifest_path: &Path, path: &str) -> PathBuf {
    let candidate = PathBuf::from(path);
    if candidate.is_absolute() {
        return candidate;
    }

    manifest_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(|parent| parent.join(&candidate))
        .unwrap_or(candidate)
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
    evidence_identities: Vec<TwinPairEvidenceIdentity>,
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
        source_materialization: snapshot.source_materialization.clone(),
        evidence_identities,
    })
}

fn observe_query(
    endpoint_id: &str,
    snapshot_hash: &str,
    catalog: &Catalog,
    backend: &BaseSnapshotBackend,
    query: &TwinPairProofQuery,
) -> TwinPairObservation {
    let result = if query.policy == "skip" {
        json!({
            "outcome_class": "skip",
            "reason_code": query.reason_code.as_deref().unwrap_or("unspecified_skip"),
            "reason": query.reason.as_deref().unwrap_or("not executed by replay policy"),
            "ordering_policy": "not_applicable",
        })
    } else {
        match query.read_op() {
            Ok(read_op) => match execute_read(catalog, backend, &read_op) {
                KernelResult::Read(read) => {
                    let row_count = read.rows.len() as u64;
                    let rows_value = serde_json::to_value(&read.rows)
                        .expect("read rows should serialize for replay hashing");
                    json!({
                        "outcome_class": "success",
                        "command_tag": format!("SELECT {row_count}"),
                        "columns": read.columns,
                        "row_count": row_count,
                        "rows_hash": sha256_json(&rows_value),
                        "ordering_policy": "kernel_deterministic_order",
                    })
                }
                KernelResult::Refusal(refusal) => json!({
                    "outcome_class": "refusal",
                    "code": refusal.code,
                    "sqlstate": refusal.sqlstate,
                    "refusal_hash": sha256_json(&serde_json::to_value(&refusal).expect("refusal should serialize")),
                    "detail": refusal.detail,
                    "ordering_policy": "not_applicable",
                }),
                other => json!({
                    "outcome_class": "unexpected",
                    "debug": format!("{other:?}"),
                    "ordering_policy": "not_applicable",
                }),
            },
            Err(message) => json!({
                "outcome_class": "refusal",
                "code": "unsupported_query_shape",
                "sqlstate": "0A000",
                "refusal_hash": sha256_json(&json!({
                    "code": "unsupported_query_shape",
                    "sqlstate": "0A000",
                    "message": message,
                })),
                "detail": {
                    "message": message,
                },
                "ordering_policy": "not_applicable",
            }),
        }
    };

    TwinPairObservation {
        endpoint_id: endpoint_id.to_owned(),
        snapshot_hash: snapshot_hash.to_owned(),
        query_id: query.id.clone(),
        result_hash: sha256_json(&result),
        result,
    }
}

fn observation_sqlstate(result: &Value) -> Option<String> {
    observation_string(result, "sqlstate")
}

fn observation_string(result: &Value, field: &str) -> Option<String> {
    result.get(field).and_then(Value::as_str).map(str::to_owned)
}

fn observation_u64(result: &Value, field: &str) -> Option<u64> {
    result.get(field).and_then(Value::as_u64)
}

fn is_skip_observation(observation: &TwinPairObservation) -> bool {
    observation
        .result
        .get("outcome_class")
        .and_then(Value::as_str)
        == Some("skip")
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

    if let Some(replay_manifest_version) = &query_file.replay_manifest_version
        && replay_manifest_version != TWIN_PAIR_REPLAY_MANIFEST_VERSION
    {
        return Err(Box::new(proof_parse(
            path,
            format!(
                "unsupported replay_manifest_version `{replay_manifest_version}` (expected `{TWIN_PAIR_REPLAY_MANIFEST_VERSION}`)"
            ),
        )));
    }

    if query_file.queries.is_empty() {
        return Err(Box::new(proof_parse(
            path,
            "twin-pair proof query file must contain at least one query",
        )));
    }

    for query in &query_file.queries {
        match query.policy.as_str() {
            "execute" => {}
            "skip" => {
                if query
                    .reason_code
                    .as_deref()
                    .is_none_or(|reason| reason.trim().is_empty())
                {
                    return Err(Box::new(proof_parse(
                        path,
                        format!("skip query `{}` must provide reason_code", query.id),
                    )));
                }
            }
            other => {
                return Err(Box::new(proof_parse(
                    path,
                    format!(
                        "query `{}` has unsupported replay policy `{}` (supported: execute, skip)",
                        query.id, other
                    ),
                )));
            }
        }
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
    #[serde(default)]
    replay_manifest_version: Option<String>,
    #[serde(default)]
    target_evidence: Vec<TwinPairEvidenceIdentity>,
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
    #[serde(default)]
    count_column: Option<String>,
    #[serde(default)]
    aggregate_alias: Option<String>,
    #[serde(default)]
    limit: Option<u64>,
    #[serde(default = "default_replay_policy")]
    policy: String,
    #[serde(default)]
    reason_code: Option<String>,
    #[serde(default)]
    reason: Option<String>,
}

fn default_replay_policy() -> String {
    String::from("execute")
}

impl TwinPairProofQuery {
    fn read_op(&self) -> Result<ReadOp, String> {
        let predicate = Some(PredicateExpr::Comparison(PredicateComparison {
            column: self.lookup_column.clone(),
            operator: PredicateOperator::Eq,
            values: vec![self.lookup_value.clone()],
        }));

        match self.shape.as_str() {
            "point_lookup" => Ok(ReadOp {
                session_id: String::from("twin-pair-proof"),
                table: self.table.clone(),
                shape: ReadShape::PointLookup,
                projection: self.projection.clone(),
                predicate,
                aggregate: AggregateSpec::default(),
                group_by: Vec::new(),
                limit: None,
            }),
            "filtered_scan" => Ok(ReadOp {
                session_id: String::from("twin-pair-proof"),
                table: self.table.clone(),
                shape: ReadShape::FilteredScan,
                projection: self.projection.clone(),
                predicate,
                aggregate: AggregateSpec::default(),
                group_by: Vec::new(),
                limit: self.limit,
            }),
            "aggregate_count" => Ok(ReadOp {
                session_id: String::from("twin-pair-proof"),
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
            _ => Err(format!(
                "query `{}` uses unsupported shape `{}` (supported: point_lookup, filtered_scan, aggregate_count)",
                self.id, self.shape
            )),
        }
    }
}
