use std::{fs, path::Path};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    cli::Engine,
    migration_proof::TwinPairEvidenceIdentity,
    refusal::{self, RefusalEnvelope, RefusalResult},
};

pub const TWIN_PAIR_ORCHESTRATION_MANIFEST_VERSION: &str =
    "twinning.twin-pair-orchestration-manifest.v0";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TwinPairOrchestrationManifest {
    pub version: String,
    pub proof_id: String,
    pub catalog_declaration: ManifestArtifactRef,
    pub left_endpoint: TwinPairEndpointSpec,
    pub right_endpoint: TwinPairEndpointSpec,
    pub replay_manifest: ManifestArtifactRef,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_evidence: Vec<TwinPairEvidenceIdentity>,
    pub artifact_outputs: TwinPairArtifactOutputs,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestArtifactRef {
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TwinPairEndpointSpec {
    pub endpoint_id: String,
    pub role: String,
    pub engine: Engine,
    pub bootstrap: TwinPairEndpointBootstrap,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TwinPairEndpointBootstrap {
    Restore {
        snapshot: String,
    },
    Schema {
        schema: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        declaration: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        load: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TwinPairArtifactOutputs {
    pub report: String,
    pub bundle_dir: String,
    pub left_snapshot: String,
    pub right_snapshot: String,
}

pub fn load_twin_pair_orchestration_manifest(
    path: &Path,
) -> RefusalResult<TwinPairOrchestrationManifest> {
    let raw = fs::read(path).map_err(|error| Box::new(refusal::io_read(path, &error)))?;
    let manifest: TwinPairOrchestrationManifest = serde_json::from_slice(&raw)
        .map_err(|error| Box::new(manifest_parse(path, error.to_string())))?;
    validate_manifest(path, &manifest)?;
    Ok(manifest)
}

fn validate_manifest(path: &Path, manifest: &TwinPairOrchestrationManifest) -> RefusalResult<()> {
    ensure(
        path,
        manifest.version == TWIN_PAIR_ORCHESTRATION_MANIFEST_VERSION,
        "manifest version must be twinning.twin-pair-orchestration-manifest.v0",
        json!({ "version": manifest.version }),
    )?;
    ensure_nonempty(path, "proof_id", &manifest.proof_id)?;
    validate_artifact_ref(path, "catalog_declaration", &manifest.catalog_declaration)?;
    validate_artifact_ref(path, "replay_manifest", &manifest.replay_manifest)?;
    validate_endpoint(path, "left_endpoint", &manifest.left_endpoint)?;
    validate_endpoint(path, "right_endpoint", &manifest.right_endpoint)?;
    ensure(
        path,
        manifest.left_endpoint.endpoint_id != manifest.right_endpoint.endpoint_id,
        "left_endpoint and right_endpoint must use distinct endpoint_id values",
        json!({
            "left_endpoint_id": manifest.left_endpoint.endpoint_id,
            "right_endpoint_id": manifest.right_endpoint.endpoint_id,
        }),
    )?;
    validate_evidence(path, &manifest.target_evidence)?;
    validate_outputs(path, &manifest.artifact_outputs)?;
    Ok(())
}

fn validate_artifact_ref(
    path: &Path,
    field: &str,
    artifact: &ManifestArtifactRef,
) -> RefusalResult<()> {
    ensure_nonempty(path, &format!("{field}.path"), &artifact.path)?;
    if let Some(hash) = &artifact.hash {
        ensure_sha256(path, &format!("{field}.hash"), hash)?;
    }
    Ok(())
}

fn validate_endpoint(
    path: &Path,
    field: &str,
    endpoint: &TwinPairEndpointSpec,
) -> RefusalResult<()> {
    ensure_nonempty(path, &format!("{field}.endpoint_id"), &endpoint.endpoint_id)?;
    ensure_nonempty(path, &format!("{field}.role"), &endpoint.role)?;
    ensure(
        path,
        endpoint.engine == Engine::Postgres,
        "twin-pair orchestration manifests are Postgres-only in this build",
        json!({ "field": field, "engine": endpoint.engine }),
    )?;

    match &endpoint.bootstrap {
        TwinPairEndpointBootstrap::Restore { snapshot } => {
            ensure_nonempty(path, &format!("{field}.bootstrap.snapshot"), snapshot)?;
        }
        TwinPairEndpointBootstrap::Schema {
            schema,
            declaration,
            load,
        } => {
            ensure_nonempty(path, &format!("{field}.bootstrap.schema"), schema)?;
            if let Some(declaration) = declaration {
                ensure_nonempty(path, &format!("{field}.bootstrap.declaration"), declaration)?;
            }
            for (index, load_path) in load.iter().enumerate() {
                ensure_nonempty(path, &format!("{field}.bootstrap.load[{index}]"), load_path)?;
            }
        }
    }
    Ok(())
}

fn validate_evidence(path: &Path, evidence: &[TwinPairEvidenceIdentity]) -> RefusalResult<()> {
    for (index, identity) in evidence.iter().enumerate() {
        ensure_nonempty(
            path,
            &format!("target_evidence[{index}].artifact_id"),
            &identity.artifact_id,
        )?;
        ensure_nonempty(
            path,
            &format!("target_evidence[{index}].version"),
            &identity.version,
        )?;
        ensure_sha256(
            path,
            &format!("target_evidence[{index}].hash"),
            &identity.hash,
        )?;
        if let Some(source) = &identity.source {
            ensure_nonempty(path, &format!("target_evidence[{index}].source"), source)?;
        }
    }
    Ok(())
}

fn validate_outputs(path: &Path, outputs: &TwinPairArtifactOutputs) -> RefusalResult<()> {
    ensure_nonempty(path, "artifact_outputs.report", &outputs.report)?;
    ensure_nonempty(path, "artifact_outputs.bundle_dir", &outputs.bundle_dir)?;
    ensure_nonempty(
        path,
        "artifact_outputs.left_snapshot",
        &outputs.left_snapshot,
    )?;
    ensure_nonempty(
        path,
        "artifact_outputs.right_snapshot",
        &outputs.right_snapshot,
    )?;
    Ok(())
}

fn ensure_nonempty(path: &Path, field: &str, value: &str) -> RefusalResult<()> {
    ensure(
        path,
        !value.trim().is_empty(),
        format!("manifest field `{field}` must not be empty"),
        json!({ "field": field }),
    )
}

fn ensure_sha256(path: &Path, field: &str, value: &str) -> RefusalResult<()> {
    let valid = value
        .strip_prefix("sha256:")
        .is_some_and(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()));
    ensure(
        path,
        valid,
        format!("manifest field `{field}` must be a sha256:<64 hex chars> identity"),
        json!({ "field": field, "value": value }),
    )
}

fn ensure(
    path: &Path,
    condition: bool,
    message: impl Into<String>,
    detail: Value,
) -> RefusalResult<()> {
    if condition {
        Ok(())
    } else {
        Err(Box::new(manifest_validation(path, message.into(), detail)))
    }
}

fn manifest_parse(path: &Path, message: impl Into<String>) -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_TWIN_PAIR_ORCHESTRATION_MANIFEST",
        format!(
            "Twin-pair orchestration manifest failed for `{}`.",
            path.display()
        ),
        json!({ "path": path.display().to_string(), "error": message.into() }),
        None,
    )
}

fn manifest_validation(path: &Path, message: String, detail: Value) -> RefusalEnvelope {
    RefusalEnvelope::new(
        "E_TWIN_PAIR_ORCHESTRATION_MANIFEST",
        format!(
            "Twin-pair orchestration manifest failed for `{}`.",
            path.display()
        ),
        json!({
            "path": path.display().to_string(),
            "error": message,
            "validation": detail,
        }),
        None,
    )
}
