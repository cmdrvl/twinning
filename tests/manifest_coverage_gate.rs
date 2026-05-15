#![forbid(unsafe_code)]

use std::{collections::BTreeMap, collections::BTreeSet, fs};

use serde::Deserialize;

#[path = "support.rs"]
mod support;

const MATRIX_VERSION: &str = "twinning.manifest-coverage-matrix.v0";
const MATRIX_PATH: &str = "tests/fixtures/canaries/manifest_coverage_matrix.json";

#[test]
fn manifest_coverage_matrix_covers_every_claimed_token() {
    let matrix = load_matrix();
    let manifest = support::manifest();

    assert_eq!(matrix.version, MATRIX_VERSION);
    assert_eq!(matrix.manifest, "canaries/manifest.v0.json");
    for skip in &matrix.skips {
        assert!(
            !skip.category.trim().is_empty() && !skip.token.trim().is_empty(),
            "coverage skips must identify category and token: {skip:?}"
        );
        assert!(
            skip.reason == "external_prerequisite",
            "coverage skip `{}:{}` used unsupported reason `{}`",
            skip.category,
            skip.token,
            skip.reason
        );
    }
    assert!(
        matrix.skips.is_empty(),
        "manifest coverage skips must stay empty unless they are external-prerequisite skips with explicit reasons: {:?}",
        matrix.skips
    );

    let entries = entries_by_key(&matrix.entries);
    for canary in &manifest.canaries {
        assert_tokens_covered(
            &entries,
            "session",
            &canary.session_shapes,
            canary.id.as_str(),
        );
        assert_tokens_covered(&entries, "write", &canary.write_shapes, canary.id.as_str());
        assert_tokens_covered(&entries, "read", &canary.read_shapes, canary.id.as_str());
        assert_tokens_covered(
            &entries,
            "sqlstate",
            &canary.required_sqlstates,
            canary.id.as_str(),
        );
        assert_token_covered(
            &entries,
            "unsupported_policy",
            canary.unsupported_policy.as_str(),
            canary.id.as_str(),
        );
    }

    let dimensions = matrix
        .entries
        .iter()
        .flat_map(|entry| entry.dimensions.iter().map(String::as_str))
        .chain(
            matrix
                .artifact_behaviors
                .iter()
                .flat_map(|entry| entry.dimensions.iter().map(String::as_str)),
        )
        .collect::<BTreeSet<_>>();
    for required in [
        "session_behavior",
        "sql_shape_success",
        "sqlstate_refusal",
        "command_tags",
        "row_counts",
        "transaction_state",
        "final_artifacts",
    ] {
        assert!(
            dimensions.contains(required),
            "manifest coverage matrix is missing required dimension `{required}`"
        );
    }

    for behavior in &matrix.artifact_behaviors {
        assert!(
            !behavior.evidence.is_empty(),
            "artifact behavior `{}` must name executable evidence",
            behavior.id
        );
        assert!(
            behavior
                .dimensions
                .iter()
                .any(|item| item == "final_artifacts"),
            "artifact behavior `{}` must cover final_artifacts",
            behavior.id
        );
    }
}

fn load_matrix() -> CoverageMatrix {
    let path = support::repo_root().join(MATRIX_PATH);
    let raw = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read coverage matrix `{}`: {error}", path.display()));
    serde_json::from_str(&raw)
        .unwrap_or_else(|error| panic!("parse coverage matrix `{}`: {error}", path.display()))
}

fn entries_by_key(entries: &[CoverageEntry]) -> BTreeMap<(&str, &str), &CoverageEntry> {
    let mut by_key = BTreeMap::new();
    for entry in entries {
        assert!(
            !entry.evidence.is_empty(),
            "coverage entry `{}:{}` must name executable evidence",
            entry.category,
            entry.token
        );
        assert!(
            !entry.dimensions.is_empty(),
            "coverage entry `{}:{}` must name covered dimensions",
            entry.category,
            entry.token
        );
        let previous = by_key.insert((entry.category.as_str(), entry.token.as_str()), entry);
        assert!(
            previous.is_none(),
            "duplicate coverage entry for `{}:{}`",
            entry.category,
            entry.token
        );
    }
    by_key
}

fn assert_tokens_covered(
    entries: &BTreeMap<(&str, &str), &CoverageEntry>,
    category: &str,
    tokens: &[String],
    canary_id: &str,
) {
    for token in tokens {
        assert_token_covered(entries, category, token, canary_id);
    }
}

fn assert_token_covered(
    entries: &BTreeMap<(&str, &str), &CoverageEntry>,
    category: &str,
    token: &str,
    canary_id: &str,
) {
    assert!(
        entries.contains_key(&(category, token)),
        "manifest canary `{canary_id}` claims `{category}:{token}` without a manifest coverage matrix entry"
    );
}

#[derive(Debug, Deserialize)]
struct CoverageMatrix {
    version: String,
    manifest: String,
    entries: Vec<CoverageEntry>,
    artifact_behaviors: Vec<ArtifactCoverageEntry>,
    skips: Vec<CoverageSkip>,
}

#[derive(Debug, Deserialize)]
struct CoverageEntry {
    category: String,
    token: String,
    evidence: Vec<String>,
    dimensions: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ArtifactCoverageEntry {
    id: String,
    evidence: Vec<String>,
    dimensions: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CoverageSkip {
    category: String,
    token: String,
    reason: String,
}
