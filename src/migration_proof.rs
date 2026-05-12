use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{cli::Engine, declaration::CatalogDeclarationIdentity};

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
